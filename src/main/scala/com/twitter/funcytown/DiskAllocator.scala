package com.twitter.funcytown

import java.io.RandomAccessFile

import com.esotericsoftware.kryo.io.{Input => KInput}
import com.esotericsoftware.kryo.io.{Output => KOutput}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}

import org.objenesis.strategy.StdInstantiatorStrategy;

import scala.annotation.tailrec
import scala.collection.mutable.{HashMap => MHash}
import scala.collection.immutable.LinearSeq

object DiskAllocator {
  def newHash[K,V](filename : String, tups : (K,V)*) = {
    HashNode.apply(tups : _*)(new DiskAllocator(filename))
  }
}

// Using the collection builder stuff, I bet you can do this correctly for anything with a builder
class ScalaListSerializer[T] extends KSerializer[List[T]] {
  override def write(k : Kryo, out : KOutput, obj : List[T]) {
    out.writeInt(obj.size, true)
    obj.foreach { item =>
      k.writeClassAndObject(out, item)
    }
  }

  override def create(k : Kryo, in : KInput, ltype : Class[List[T]]) : List[T] = {
    val sz = in.readInt(true)
    (0 until sz).foldLeft(List[T]()) { (oldList, idx) =>
      k.readClassAndObject(in).asInstanceOf[T] :: oldList
    }.reverse // reverse the stack to get the original order
  }
}

class SingletonSerializer[T](inst : T) extends KSerializer[T] {
  override def write(k : Kryo, out : KOutput, obj : T) {
    assert(inst == obj, "Singleton serializer only works for one instance")
    // Do nothing
  }
  override def create(k : Kryo, in : KInput, stype : Class[T]) : T = inst
}

class DiskAllocator(filename : String) extends Allocator[Long] {

  val kryo = new Kryo
  // Accept everything for now:
  kryo.setRegistrationRequired(false)
  kryo.addDefaultSerializer(classOf[List[AnyRef]], new ScalaListSerializer[AnyRef])
  // Put in some singletons:
  kryo.register(this.getClass, new SingletonSerializer(this))
  kryo.register(None.getClass, new SingletonSerializer(None))
  kryo.register(Nil.getClass, new SingletonSerializer(Nil))
  // Use objensis for better support of scala objects:
  kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

  // TODO we can probably read and write independently
  private val file = new RandomAccessFile(filename,"rw")
  // We have to init to avoid writing something in the "null" offset of 0:
  file.writeInt(0x5ca1ab1e)
  // Where to seek before each write, only changed inside the file lock
  private var eofPtr = file.getFilePointer
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
    val (objType, buf) = file.synchronized {
      file.seek(ptr)
      val objType = file.readByte
      val len = file.readInt
      val buf = new Array[Byte](len)
      file.readFully(buf)
      (objType, buf)
    }
    objType match {
      // This is ugly, but this method is not type safe anyway
      case LEAFNODE => readLeaf[AnyRef](ptr, buf)
      case PTRNODE => readPtrNode[AnyRef](ptr, buf)
      case SEQNODE => readSeqNode[AnyRef](ptr, buf)
      case _ => error("Unrecognized node type: " + objType)
    }
  }

  protected def readLeaf[T](ptr : Long, buf : Array[Byte]) : DiskLeaf[T] = {
    val input = new KInput(buf)
    // These bool parameters mean optimize for positive sizes:
    val height = input.readShort(true)
    val pos = input.readLong(true)
    val obj = kryo.readClassAndObject(input).asInstanceOf[T]
    new DiskLeaf[T](ptr, height, pos, obj, this)
  }

  protected def readPtrNode[T](ptr : Long, buf : Array[Byte]) : DiskPtrNode[T] = {
    val input = new KInput(buf)
    // These bool parameters mean optimize for positive sizes:
    val treeSize = input.readLong(true)
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
    new DiskPtrNode[T](ptr, treeSize, height, blk, this)
  }

  protected def readSeqNode[T](ptr : Long, buf : Array[Byte]) : DiskSeq[T] = {
    val input = new KInput(buf)
    // These bool parameters mean optimize for positive sizes:
    val tail = input.readLong(true)
    val head = kryo.readClassAndObject(input).asInstanceOf[T]
    new DiskSeq[T](ptr, head, tail, this)
  }

  override def empty[T](height : Short) : PtrNode[T,Long] = {
    emptyPtrNodes.synchronized {
      emptyPtrNodes.getOrElseUpdate(height,
        allocPtrNode(0L, height, Block.alloc[Long])
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

  override def ptrOf[T](seq : SeqNode[T,Long]) = {
    seq.asInstanceOf[DiskSeq[T]].ptr
  }

  protected val NIL = allocSeq[AnyRef](null, nullPtr)
  override def nil[T] : SeqNode[T,Long] = NIL.asInstanceOf[SeqNode[T,Long]]
  override def allocSeq[T](h : T, tail : Long) : SeqNode[T,Long] = {
    val toWrite = output.synchronized {
      output.clear
      output.writeLong(tail, true)
      kryo.writeClassAndObject(output, h)
      output.toBytes
    }
    val ptr = writeBytes(SEQNODE, toWrite)
    new DiskSeq[T](ptr, h, tail, this)
  }

  protected def writeBytes(objType : Byte, toWrite : Array[Byte]) : Long = {
    file.synchronized {
      val thisPtr = eofPtr
      file.seek(eofPtr)
      file.writeByte(objType)
      file.writeInt(toWrite.length)
      file.write(toWrite)
      // Remember where to seek to before next write
      eofPtr = file.getFilePointer
      thisPtr
    }
  }

  override def allocLeaf[T](height : Short, pos : Long, value : T) = {
    val toWrite = output.synchronized {
      output.clear
      output.writeShort(height, true)
      output.writeLong(pos, true)
      kryo.writeClassAndObject(output, value)
      output.toBytes
    }
    val ptr = writeBytes(LEAFNODE, toWrite)
    new DiskLeaf[T](ptr, height, pos, value, this)
  }

  override def allocPtrNode[T](sz : Long, height : Short, ptrs : Block[Long]) = {
    val toWrite = output.synchronized {
      output.clear
      output.writeLong(sz, true)
      output.writeShort(height, true)
      ptrs.foldLeft(output) { (oldOut, thisVal) =>
        oldOut.writeLong(thisVal, true)
        oldOut
      }
      output.toBytes
    }
    val ptr = writeBytes(PTRNODE, toWrite)
    new DiskPtrNode[T](ptr, sz, height, ptrs, this)
  }
}

class DiskLeaf[T](val ptr : Long, hs : Short, ps : Long, v : T, m : Allocator[Long]) extends
  Leaf[T,Long](hs, ps, v, m)

class DiskPtrNode[T](val ptr : Long, sz : Long, height : Short, ptrs : Block[Long],
  mem : Allocator[Long]) extends PtrNode[T,Long](sz, height, ptrs, mem)

class DiskSeq[T](val ptr : Long, h : T, t : Long, mem : Allocator[Long])
  extends SeqNode[T,Long](h, t, mem)
