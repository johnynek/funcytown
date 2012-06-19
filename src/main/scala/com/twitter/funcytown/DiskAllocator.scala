package com.twitter.funcytown

import java.io.RandomAccessFile

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

// Using the collection builder stuff, I bet you can do this correctly for anything with a builder
class ScalasciListSerializer[T] extends KSerializer[sciList[T]] {
  override def write(k : Kryo, out : KOutput, obj : sciList[T]) {
    out.writeInt(obj.size, true)
    obj.foreach { item =>
      k.writeClassAndObject(out, item)
    }
  }

  override def create(k : Kryo, in : KInput, ltype : Class[sciList[T]]) : sciList[T] = {
    val sz = in.readInt(true)
    (0 until sz).foldLeft(sciList[T]()) { (oldsciList, idx) =>
      k.readClassAndObject(in).asInstanceOf[T] :: oldsciList
    }.reverse // reverse the stack to get the original order
  }
}

class HashEntrySerializer[K,V](hashfn : K => Long) extends KSerializer[HashEntry[K,V]] {
  override def write(k : Kryo, out : KOutput, obj : HashEntry[K,V]) {
    k.writeClassAndObject(out, obj.key.asInstanceOf[AnyRef])
    k.writeClassAndObject(out, obj.value.asInstanceOf[AnyRef])
  }
  override def create(k : Kryo, in : KInput, clazz : Class[HashEntry[K,V]]) : HashEntry[K,V] = {
    val key = k.readClassAndObject(in).asInstanceOf[K]
    val value = k.readClassAndObject(in).asInstanceOf[V]
    new HashEntry(hashfn(key), key, value)
  }
}

class SingletonSerializer[T](inst : T) extends KSerializer[T] {
  override def write(k : Kryo, out : KOutput, obj : T) {
    assert(inst == obj, "Singleton serializer only works for one instance")
    // Do nothing
  }
  override def create(k : Kryo, in : KInput, stype : Class[T]) : T = inst
}

trait ByteStorage {
  // read the data at pos and return the header byte, and the header byte, and bytearray
  def readBytes(pos : Long) : (Byte, Array[Byte])
  // the inverse of the above
  def writeBytes(header : Byte, toWrite : Array[Byte]) : Long
}

trait FileStorage extends ByteStorage {

  val file : RandomAccessFile
  // Where to seek before each write, only changed inside the file lock
  private val eofPtrLock = new Object
  private var eofPtr = 0L
  protected val minPtr = 4 //we write a magic int in the header

  def close {
    file.synchronized { file.close }
  }

  def init {
    // We have to init to avoid writing something in the "null" offset of 0:
    file.writeInt(0x5ca1ab1e)
    eofPtr = file.getFilePointer
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
      file.synchronized {
        file.seek(ptr)
        val objType = file.readByte
        val len = file.readInt
        val buf = new Array[Byte](len)
        file.readFully(buf)
        (objType, buf)
      }
    }
  }

  def sizeOf(ptr : Long) : Int = {
    retryOnEOF {
      file.synchronized {
        file.seek(ptr + 1) // Skip the object header
        // header + size + data
        1 + 4 + file.readInt
      }
    }
  }

  @inline
  protected def sizeOf(objType : Byte, toWrite : Array[Byte]) = (1 + 4 + toWrite.size)
  // This finds a pointer without blocking on the file lock
  // in principle, this allows a separate thread to handle writing
  protected def getPointer(objType : Byte, toWrite : Array[Byte]) : Long = {
    eofPtrLock.synchronized {
      val thisPtr = eofPtr
      // update the pointer:
      eofPtr = thisPtr + sizeOf(objType, toWrite)
      thisPtr
    }
  }

  def writeAt(pos : Long, objType : Byte, toWrite : Array[Byte]) {
    file.synchronized {
      file.seek(pos)
      file.writeByte(objType)
      file.writeInt(toWrite.length)
      file.write(toWrite)
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
            writeCache.synchronized { writeCache -= pos }
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
    // TODO multiple writes to the same ptr could cause a failure, need to do a refcount approach
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

  val kryo = new Kryo
  // Accept everything for now:
  kryo.setRegistrationRequired(false)
  kryo.addDefaultSerializer(classOf[sciList[AnyRef]], new ScalasciListSerializer[AnyRef])
  // Put in some singletons:
  kryo.register(this.getClass, new SingletonSerializer(this))
  kryo.register(None.getClass, new SingletonSerializer(None))
  kryo.register(Nil.getClass, new SingletonSerializer(Nil))
  kryo.register(classOf[HashEntry[AnyRef,AnyRef]], new HashEntrySerializer[AnyRef,AnyRef](_.hashCode.toLong))
  // Use objensis for better support of scala objects:
  kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

  private val output = new KOutput(4096, 1 << 20)
  private val emptyPtrNodes = MHash[Short, DiskPtrNode[_]]()

  val LEAFNODE = 1 : Byte
  val PTRNODE = 2 : Byte
  val SEQNODE = 3 : Byte

  override val nullPtr : Long = 0L
  override def deref(ptr : Long) = {
    if (ptr == 0L) {
      error("Trying to deref 0")
    }
    val (objType, buf) = readBytes(ptr)
    objType match {
      // This is ugly, but this method is not type safe anyway
      case LEAFNODE => readLeaf[AnyRef](ptr, buf)
      case PTRNODE => readPtrNode[AnyRef](ptr, buf)
      case SEQNODE => readList[AnyRef](ptr, buf)
      case _ => error("Unrecognized node type: " + objType)
    }
  }

  protected def readLeaf[T](ptr : Long, buf : Array[Byte]) : DiskLeaf[T] = {
    val input = new KInput(buf)
    // These bool parameters mean optimize for positive sizes:
    val height = input.readShort(true)
    val pos = input.readLong(true)
    val obj = kryo.synchronized { kryo.readClassAndObject(input).asInstanceOf[T] }
    new DiskLeaf[T](ptr, height, pos, obj, this)
  }

  protected def readPtrNode[T](ptr : Long, buf : Array[Byte]) : DiskPtrNode[T] = {
    val input = new KInput(buf)
    // These bool parameters mean optimize for positive sizes:
    val height = input.readShort(true)
    // Read the array:
    val blockAry = new Array[Long](Block.BITMASK + 1)
    // I know this is not hip, but it is fast:
    var idx = 0
    while( idx < (Block.BITMASK + 1)) {
      blockAry(idx) = input.readLong(true)
      idx = idx + 1
    }
    val blk = new Block[Long](Block.BITMASK, blockAry)
    new DiskPtrNode[T](ptr, height, blk, this)
  }

  protected def readList[T](ptr : Long, buf : Array[Byte]) : DiskSeq[T] = {
    val input = new KInput(buf)
    // These bool parameters mean optimize for positive sizes:
    val tail = input.readLong(true)
    val head = kryo.synchronized { kryo.readClassAndObject(input).asInstanceOf[T] }
    new DiskSeq[T](ptr, head, tail, this)
  }

  override def empty[T](height : Short) : PtrNode[T,Long] = {
    emptyPtrNodes.synchronized {
      emptyPtrNodes.getOrElseUpdate(height,
        allocPtrNode(height, Block.alloc[Long])
      )
      .asInstanceOf[PtrNode[T,Long]]
    }
  }

  override def ptrOf[T](node : Node[T,Long]) = {
    node match {
      case leaf : DiskLeaf[_] => leaf.ptr
      case ptrNode : DiskPtrNode[_] => ptrNode.ptr
      case _ => error("Invalid node")
    }
  }

  override def ptrOf[T](seq : List[T,Long]) = {
    seq.asInstanceOf[DiskSeq[T]].ptr
  }

  protected lazy val NIL = allocSeq[AnyRef](null, nullPtr)
  override def nil[T] : List[T,Long] = NIL.asInstanceOf[List[T,Long]]

  override def allocSeq[T](h : T, tail : Long) : List[T,Long] = {
    val toWrite = output.synchronized {
      output.clear
      output.writeLong(tail, true)
      kryo.synchronized { kryo.writeClassAndObject(output, h) }
      output.toBytes
    }
    val ptr = writeBytes(SEQNODE, toWrite)
    afterAlloc(ptr, new DiskSeq[T](ptr, h, tail, this))
  }

  override def allocLeaf[T](height : Short, pos : Long, value : T) = {
    val toWrite = output.synchronized {
      output.clear
      output.writeShort(height, true)
      output.writeLong(pos, true)
      kryo.synchronized { kryo.writeClassAndObject(output, value) }
      output.toBytes
    }
    val ptr = writeBytes(LEAFNODE, toWrite)
    afterAlloc(ptr, new DiskLeaf[T](ptr, height, pos, value, this))
  }

  override def allocPtrNode[T](height : Short, ptrs : Block[Long]) = {
    val toWrite = output.synchronized {
      output.clear
      output.writeShort(height, true)
      ptrs.foldLeft(output) { (oldOut, thisVal) =>
        oldOut.writeLong(thisVal, true)
        oldOut
      }
      output.toBytes
    }
    val ptr = writeBytes(PTRNODE, toWrite)
    afterAlloc(ptr, new DiskPtrNode[T](ptr, height, ptrs, this))
  }
}

class DiskAllocator(filename : String) extends ByteAllocator with FileStorage {
  override val file = new RandomAccessFile(filename,"rw")
  // initialize the file:
  this.init

  override def finalize = close
}

// Caches the actual objects, not the bytes read
abstract class CachingByteAllocatorBase(cachedItems : Int) extends ByteAllocator {

  protected val cache = new LRUMap(cachedItems)

  override def deref(ptr : Long) : AnyRef = {
    val boxedPtr = ptr.asInstanceOf[AnyRef]
    val cached = cache.synchronized { cache.get(boxedPtr) }
    if (cached == null) {
      //We need to actually read off disk:
      val toCache = super.deref(ptr)
      cache.synchronized { cache.put(boxedPtr, toCache) }
      toCache
    }
    else {
      // return the cached value:
      cached
    }
  }

  override def afterAlloc[T](ptr : Long, obj : T) : T = {
    val boxedPtr = ptr.asInstanceOf[AnyRef]
    // Make sure the allocation is in the read cache, since writing is async
    cache.synchronized { cache.put(boxedPtr, obj) }
    // We are the first sublass under ByteAllocator, so we are done with the afterAlloc chain:
    obj
  }
}

class CachingDiskAllocator(cachedItems : Int, filename : String = null)
  extends CachingByteAllocatorBase(cachedItems) with AsyncWriterStorage {

  // TODO check file.deleteOnExit() to make sure this gets cleaned up
  private val realFileName = Option(filename).getOrElse(java.util.UUID.randomUUID.toString)
  override val file = new RandomAccessFile(realFileName,"rw")

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

class GCDiskAllocator(cachedItems : Int, filename : String = null)
  extends CachingByteAllocatorBase(cachedItems) with GCFileStorage {

  override val gcIntervalBytes = 1L << 20 // 1 MiB
  // TODO check file.deleteOnExit() to make sure this gets cleaned up
  private val realFileName = Option(filename).getOrElse(java.util.UUID.randomUUID.toString)
  override val file = new RandomAccessFile(realFileName,"rw")
  // initialize the file, start the write thread
  this.init

  override def afterAlloc[T](ptr : Long, obj : T) : T = {
    // When we alloc we pin, when we free, we unpin
    addToPinned(ptr)
    super.afterAlloc(ptr, obj)
  }

  override def finalize { close }

  override def close {
    super.close
    // Delete the file if we need to:
    if (filename == null) {
      (new java.io.File(realFileName)).delete
    }
  }
}


/*
 * Anything implementing this trait is both immutable and part of an
 * acyclic graph, so when finding the reachable set, we don't have to worry
 * about that set changing in time
 */
trait ImmutableDagNode[PtrT] {
  def pointers : Set[PtrT]

  // Allows a node with data to recurse on that data
  // Be careful: if you mix nodes of different ptr types,
  // due to type erasure of the PtrT, this method is not safe.
  def pointersOf[T](t : T) : Set[PtrT] = {
    if(t.isInstanceOf[ImmutableDagNode[PtrT]]) {
      //We recurse:
      t.asInstanceOf[ImmutableDagNode[PtrT]].pointers
    }
    else {
      Set[PtrT]()
    }
  }
}

class DiskLeaf[T](val ptr : Long, hs : Short, ps : Long, v : T, m : Allocator[Long]) extends
  Leaf[T,Long](hs, ps, v, m) with ImmutableDagNode[Long] {
  override lazy val pointers = pointersOf(v)
  override def finalize { m.free(ptr) }
}

class DiskPtrNode[T](val ptr : Long, height : Short, ptrs : Block[Long],
  mem : Allocator[Long]) extends PtrNode[T,Long](height, ptrs, mem) with ImmutableDagNode[Long] {
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

class DiskSeq[T](val ptr : Long, h : T, t : Long, mem : Allocator[Long])
  extends List[T,Long](h, t, mem) with ImmutableDagNode[Long] {
  override lazy val pointers = {
    val ptrs = pointersOf(h)
    if (t != mem.nullPtr) { ptrs + t } else { ptrs }
  }
  override def finalize { mem.free(ptr) }
}
