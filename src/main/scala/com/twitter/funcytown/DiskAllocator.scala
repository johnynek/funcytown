package com.twitter.funcytown

import com.esotericsoftware.kryo.io.{Input => KInput}
import com.esotericsoftware.kryo.io.{Output => KOutput}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}

import org.objenesis.strategy.StdInstantiatorStrategy;

import scala.collection.mutable.{HashMap => MHash}
import scala.collection.immutable.LinearSeq
import scala.collection.immutable.{List => sciList}
import scala.actors.Actor
import scala.actors.Actor._
import scala.annotation.tailrec

import org.apache.commons.collections.LRUMap

trait ByteStorage {
  // read the data at pos and return the header byte, and the header byte, and bytearray
  def readBytes(pos : Long) : (Byte, Array[Byte])
  // the inverse of the above
  def writeBytes(header : Byte, toWrite : Array[Byte]) : Long
}

trait FileStorage extends ByteStorage {
  // Start out 1L, because 0L is used for null
  // This is used by the GC
  protected val minPtr = 1L
  private val eofPtr = new SyncVar[Long](minPtr)
  protected val file : SyncVar[FileLike]

  def close { file.mutate { _.close } }

  def init {
    // Make sure 0L is never allocated:
    file.mutate { _.writeByte(0.toByte) }
  }

  @tailrec
  protected final def retryOnEOF[T]( fn : => T ) : T = {
    val res = try { Some(fn) }
    catch {
      case x: java.io.EOFException => {
        //This happens if the write hasn't happened by the time we need it
        Thread.sleep(50) //Sleep 50 ms and try again:
        None
      }
    }
    if (res.isDefined) res.get else retryOnEOF(fn)
  }

  def readBytes(ptr : Long) : (Byte, Array[Byte]) = {
    retryOnEOF {
      file.effect { f =>
        val (objType, fl1) = f.seek(ptr).readByte
        val (len, f2) = fl1.readInt
        val buf = new Array[Byte](len)
        // Now update the file
        (f2.readFully(buf), (objType, buf))
      }
    }
  }

  def sizeOf(ptr : Long) : Int = {
    retryOnEOF {
      file.effect { f =>
        val dataSizeFl = f.seek(ptr + 1).readInt // Skip the object header
        // header + size + data
        (dataSizeFl._2, 1 + 4 + dataSizeFl._1)
      }
    }
  }

  @inline
  protected def sizeOf(objType : Byte, toWrite : Array[Byte]) = (1 + 4 + toWrite.size)
  // This finds a pointer without blocking on the file lock
  // in principle, this allows a separate thread to handle writing
  protected def getPointer(objType : Byte, toWrite : Array[Byte]) : Long = {
    eofPtr.effect { eof =>
      // update the pointer, and read it out
      (eof + sizeOf(objType, toWrite), eof)
    }
  }

  def writeAt(pos : Long, objType : Byte, toWrite : Array[Byte]) {
    file.mutate { fl : FileLike =>
      fl.seek(pos)
        .writeByte(objType)
        .writeInt(toWrite.length)
        .writeFully(toWrite)
    }
  }

  def writeBytes(objType : Byte, toWrite : Array[Byte]) : Long = {
    val ptr = getPointer(objType, toWrite)
    writeAt(ptr, objType, toWrite)
    ptr
  }
}

trait AsyncWriterStorage extends FileStorage {
  case class WriteRecord(pos : Long, objType : Byte, data : Array[Byte])
  case object Stop

  private val writeCache = new MHash[Long, (Byte, Array[Byte])]()

  protected val writingActor = new Actor {

    def act() {
      loop {
        react {
          case WriteRecord(pos,objType,data) => {
            writeAt(pos, objType, data)
            // Now we have written, remove from the cache:
            writeCache.synchronized {
              writeCache.get(pos).map { objTypeBytes =>
                if(objTypeBytes == (objType,data)) {
                  // This is the record we just wrote:
                  writeCache -= pos
                }
                // else, another write overwrote this position, we'll clean
                // when we actually write that data
              }
            }
          }
          case Stop => { close; exit }
        }
      }
    }
  }

  override def init {
    super.init
    writingActor.start
  }

  def stop { writingActor ! Stop }
  override def sizeOf(ptr : Long) : Int = {
    writeCache.synchronized { writeCache.get(ptr) }
      .map { _._2.size + 1 }
      .getOrElse { super.sizeOf(ptr) }
  }
  // There is a race if we don't cache the data until it is written
  override def readBytes(ptr : Long) : (Byte, Array[Byte]) = {
    writeCache.synchronized { writeCache.get(ptr) }
      .getOrElse { super.readBytes(ptr) }
  }
  override def writeBytes(objType : Byte, toWrite : Array[Byte]) : Long = {
    val ptr = getPointer(objType, toWrite)
    writeCache.synchronized { writeCache += (ptr -> (objType, toWrite)) }
    writingActor ! WriteRecord(ptr, objType, toWrite)
    ptr
  }
}

trait ByteCachingStorage extends AsyncWriterStorage {
  // write into a cache, use akka to have an actor own the cache
  // read from the cache, on misses hit file, fill cache
  val cachedItems : Int

  protected val cache = new LRUMap(cachedItems)

  override def readBytes(ptr : Long) : (Byte, Array[Byte]) = {
    val boxedPtr = ptr.asInstanceOf[AnyRef]
    val cached = cache.synchronized { cache.get(boxedPtr) }
    if (cached == null) {
      //We need to actually read off disk:
      val toCache = super.readBytes(ptr)
      cache.synchronized { cache.put(boxedPtr, toCache) }
      toCache
    }
    else {
      // return the cached value:
      cached.asInstanceOf[(Byte,Array[Byte])]
    }
  }

  override def writeBytes(objType : Byte, toWrite : Array[Byte]) : Long = {
    val ptr = super.writeBytes(objType, toWrite)
    cache.synchronized { cache.put(ptr.asInstanceOf[AnyRef], (objType, toWrite)) }
    ptr
  }

}

abstract class ByteAllocator extends Allocator[Long] with ByteStorage {

  val kryo = { val k = new Kryo
    // Accept everything for now:
    k.setRegistrationRequired(false)
    k.addDefaultSerializer(classOf[sciList[AnyRef]], new ScalasciListSerializer[AnyRef])
    // Put in some singletons:
    k.register(this.getClass, new SingletonSerializer(this))
    k.register(None.getClass, new SingletonSerializer(None))
    k.register(Nil.getClass, new SingletonSerializer(Nil))
    k.register(classOf[HashEntry[AnyRef,AnyRef]],
      new HashEntrySerializer[AnyRef,AnyRef](_.hashCode.toLong))
    // Use our byte-storage to just serialize the pointers, and call deref on deserialize
    k.register(classOf[ObjNode[AnyRef]], new IdnSerializer[ObjNode[AnyRef]](this))
    k.register(classOf[DiskLeaf], new IdnSerializer[DiskLeaf](this))
    k.register(classOf[DiskPtrNode], new IdnSerializer[DiskPtrNode](this))
    k.register(classOf[DiskSeq[AnyRef]], new IdnSerializer[DiskSeq[AnyRef]](this))
    // Use objensis for better support of scala objects:
    k.setInstantiatorStrategy(new StdInstantiatorStrategy());
    k
  }

  private val output = new KOutput(4096, 1 << 20)
  private val emptyPtrNodes = MHash[Short, DiskPtrNode]()

  val LEAFNODE = 1 : Byte
  val PTRNODE = 2 : Byte
  val SEQNODE = 3 : Byte
  val OBJNODE = 4 : Byte

  override val nullPtr : Long = 0L
  override def deref[T](ptr : Long) = {
    if (ptr == 0L) {
      error("Trying to deref 0")
    }
    val (objType, buf) = readBytes(ptr)
    val obj = objType match {
      // This is ugly, but this method is not type safe anyway
      case LEAFNODE => readLeaf(ptr, buf)
      case PTRNODE => readPtrNode(ptr, buf)
      case SEQNODE => readList[AnyRef](ptr, buf)
      case OBJNODE => readObj[AnyRef](ptr, buf)
      case _ => error("Unrecognized node type: " + objType)
    }
    obj.asInstanceOf[T]
  }

  override def derefObj[T](ptr : Long) : T = deref[ObjNode[T]](ptr).obj

  protected def readLeaf(ptr : Long, buf : Array[Byte]) : DiskLeaf = {
    val input = new KInput(buf)
    // These bool parameters mean optimize for positive sizes:
    val height = input.readShort
    val pos = input.readLong(true)
    val valPtr = input.readLong(true)
    new DiskLeaf(ptr, height, pos, valPtr, this)
  }

  protected def readPtrNode(ptr : Long, buf : Array[Byte]) : DiskPtrNode = {
    val input = new KInput(buf)
    // These bool parameters mean optimize for positive sizes:
    val height = input.readShort
    // Read the array:
    val sparsity = input.readInt
    // If sparsity is -1, that means we are not sparse:
    val blk = if( sparsity != -1 ) {
      Block.fromSparse[Long](
        (0 until sparsity).foldLeft(Map[Int,Long]()) { (map, idx) =>
          val key = input.readInt(true)
          val value = input.readLong(true)
          map + (key -> value)
        }, 0L)
      }
      else {
        // Dense representation:
        val blockAry = new Array[Long](Block.BITMASK + 1)
        (0 to Block.BITMASK).foreach { idx =>
          blockAry(idx) = input.readLong(true)
        }
        new Block[Long](Block.BITMASK, blockAry)
      }
    new DiskPtrNode(ptr, height, blk, this)
  }

  protected def readList[T](ptr : Long, buf : Array[Byte]) : DiskSeq[T] = {
    val input = new KInput(buf)
    // These bool parameters mean optimize for positive sizes:
    val objptr = input.readLong(true)
    val tail = input.readLong(true)
    new DiskSeq[T](ptr, objptr, tail, this)
  }

  protected def readObj[T](ptr : Long, buf : Array[Byte]) : ObjNode[T] = {
    val input = new KInput(buf)
    val head = kryo.synchronized { kryo.readClassAndObject(input).asInstanceOf[T] }
    new ObjNode[T](ptr, head, this)
  }

  override def empty(height : Short) : PtrNode[Long] = {
    emptyPtrNodes.synchronized {
      emptyPtrNodes.getOrElseUpdate(height,
        allocPtrNode(height, Block.alloc[Long])
      )
    }
  }

  override def ptrOf(node : Node[Long]) = {
    node match {
      case leaf : DiskLeaf => leaf.ptr
      case ptrNode : DiskPtrNode => ptrNode.ptr
      case _ => error("Invalid node")
    }
  }

  override def ptrOf[T](seq : List[T,Long]) = {
    seq.asInstanceOf[DiskSeq[T]].ptr
  }

  protected lazy val NIL = allocCons(nullPtr, nullPtr)
  override def nil[T] : List[T,Long] = NIL.asInstanceOf[List[T,Long]]

  override def allocObj[T](obj : T) : Long = {
    val toWrite = output.synchronized {
      output.clear
      kryo.synchronized { kryo.writeClassAndObject(output, obj) }
      output.toBytes
    }
    val ptr = writeBytes(OBJNODE, toWrite)
    // Add to the cache or pinned set
    afterAlloc(ptr, new ObjNode[T](ptr, obj, this))
    ptr
  }

  override def allocCons[T](h : Long, tail : Long) : List[T,Long] = {
    val toWrite = output.synchronized {
      output.clear
      output.writeLong(h, true)
      output.writeLong(tail, true)
      output.toBytes
    }
    val ptr = writeBytes(SEQNODE, toWrite)
    afterAlloc(ptr, new DiskSeq[T](ptr, h, tail, this))
  }

  override def allocLeaf(height : Short, pos : Long, value : Long) = {
    val toWrite = output.synchronized {
      output.clear
      output.writeShort(height)
      output.writeLong(pos, true)
      output.writeLong(value, true)
      output.toBytes
    }
    val ptr = writeBytes(LEAFNODE, toWrite)
    afterAlloc(ptr, new DiskLeaf(ptr, height, pos, value, this))
  }

  override def allocPtrNode(height : Short, ptrs : Block[Long]) = {
    val toWrite = output.synchronized {
      output.clear
      output.writeShort(height)
      val sparse = ptrs.toSparse(0L)
      if (sparse.size < (Block.BITMASK / 2)) {
        // Use the sparse representation:
        output.writeInt(sparse.size)
        sparse.foreach { (kv) =>
          output.writeInt(kv._1, true)
          output.writeLong(kv._2, true)
        }
      }
      else {
        // Signal that we are dense:
        output.writeInt(-1)
        // Use the fold as a loop:
        ptrs.foldLeft(output) { (out, thisVal) =>
          out.writeLong(thisVal, true)
          out
        }
      }
      output.toBytes
    }
    val ptr = writeBytes(PTRNODE, toWrite)
    afterAlloc(ptr, new DiskPtrNode(ptr, height, ptrs, this))
  }
}

class DiskAllocator(filename : String, spillSize : Int)
  extends ByteAllocator with FileStorage {
  override val file = new SyncVar[FileLike](FileLikeSeq(MemoryFileLike(spillSize),
    RandAccessFileLike(filename)))
  // initialize the file:
  this.init

  override def finalize = close
}

// Caches the actual objects, not the bytes read
abstract class CachingByteAllocatorBase(cachedItems : Int) extends ByteAllocator {

  protected val cache = new LRUMap(cachedItems)

  override def deref[T](ptr : Long) = {
    val boxedPtr = ptr.asInstanceOf[AnyRef]
    val cached = cache.synchronized { cache.get(boxedPtr) }
    if (cached == null) {
      //We need to actually read off disk:
      val toCache = super.deref[T](ptr)
      cache.synchronized { cache.put(boxedPtr, toCache) }
      toCache
    }
    else {
      // return the cached value:
      cached
    }.asInstanceOf[T]
  }

  override def afterAlloc[T](ptr : Long, obj : T) : T = {
    val boxedPtr = ptr.asInstanceOf[AnyRef]
    // Make sure the allocation is in the read cache, since writing is async
    cache.synchronized { cache.put(boxedPtr, obj) }
    // We are the first sublass under ByteAllocator, so we are done with the afterAlloc chain:
    obj
  }
}

// Keep 10MB in memory before going to disk by default
class CachingDiskAllocator(cachedItems : Int, spillSize : Int = 10000000, filename : String = null)
  extends CachingByteAllocatorBase(cachedItems) with AsyncWriterStorage {

  // TODO check file.deleteOnExit() to make sure this gets cleaned up
  private val realFileName = Option(filename).getOrElse(java.util.UUID.randomUUID.toString)
  override val file = new SyncVar[FileLike](FileLikeSeq(MemoryFileLike(spillSize),
    RandAccessFileLike(realFileName)))

  // initialize the file, start the write thread
  this.init

  override def finalize { close }

  override def close {
    super.close
    // Delete the file if we need to:
    if (filename == null) {
      (new java.io.File(realFileName)).delete
    }
  }
}

class ObjNode[T](val ptr : Long, val obj : T, override val mem : Allocator[Long]) extends ImmutableDagNode[Long] {
  override def selfPtr = ptr
  override lazy val pointers = pointersOf(obj)
  override def finalize { mem.free(ptr) }
}

class DiskLeaf(val ptr : Long, hs : Short, ps : Long, valuePtr : Long, override val mem : Allocator[Long]) extends
  Leaf[Long](hs, ps, valuePtr, mem) with ImmutableDagNode[Long] {
  override def selfPtr = ptr
  override lazy val pointers = Set(valuePtr).filter { _ != mem.nullPtr }
  override def finalize { mem.free(ptr) }
}

class DiskPtrNode(val ptr : Long, height : Short, ptrs : Block[Long],
  override val mem : Allocator[Long])
  extends PtrNode[Long](height, ptrs, mem) with ImmutableDagNode[Long] {
  override def selfPtr = ptr
  override lazy val pointers = ptrs.foldLeft(Set[Long]()) { (set, ptr) =>
    if (ptr != mem.nullPtr) {
      set + ptr
    }
    else {
      set
    }
  }
  override def finalize { mem.free(ptr) }
}

class DiskSeq[T](val ptr : Long, h : Long, t : Long, override val mem : Allocator[Long])
  extends List[T,Long](h, t, mem) with ImmutableDagNode[Long] {
  override def selfPtr = ptr
  override lazy val pointers = Set(h,t).filter { _ != mem.nullPtr }
  override def finalize { mem.free(ptr) }
}
