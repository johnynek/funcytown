package org.bykn.funcytown.disk

import org.bykn.funcytown._
import org.bykn.funcytown.storage.ByteStorage

import com.esotericsoftware.kryo.io.{Input => KInput}
import com.esotericsoftware.kryo.io.{Output => KOutput}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}

import org.objenesis.strategy.StdInstantiatorStrategy;

import scala.collection.mutable.{HashMap => MHash}
import scala.collection.immutable.{List => sciList}

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
    k.register(classOf[DiskPtrNode], new IdnSerializer[DiskPtrNode](this))
    k.register(classOf[DiskSeq[AnyRef]], new IdnSerializer[DiskSeq[AnyRef]](this))
    // Use objensis for better support of scala objects:
    k.setInstantiatorStrategy(new StdInstantiatorStrategy());
    k
  }

  private val output = new KOutput(4096, 1 << 20)
  private val emptyPtrNodes = MHash[Short, DiskPtrNode]()

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
      case PTRNODE => readPtrNode(ptr, buf)
      case SEQNODE => readList[AnyRef](ptr, buf)
      case OBJNODE => readObj[AnyRef](ptr, buf)
      case _ => error("Unrecognized node type: " + objType)
    }
    obj.asInstanceOf[T]
  }

  override def derefObj[T](ptr : Long) : T = deref[ObjNode[T]](ptr).obj

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
