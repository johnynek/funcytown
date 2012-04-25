package com.twitter.funcytown

import java.io.RandomAccessFile

import com.esotericsoftware.kryo.io.{Input => KInput}
import com.esotericsoftware.kryo.io.{Output => KOutput}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}

import org.objenesis.strategy.StdInstantiatorStrategy;

import scala.annotation.tailrec
import scala.collection.mutable.{HashMap => MHash}

object DiskAllocator {
  def newHash[K,V](filename : String, tups : (K,V)*)(implicit mf : Manifest[List[(Long,K,V)]]) = {
    HashNode.apply(tups : _*)(new DiskAllocator[List[(Long,K,V)]](filename))
  }
}

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

class DiskAllocator[T](filename : String)(implicit mf : Manifest[T]) extends Allocator[T, Long] {

  val kryo = new Kryo
  // Accept everything for now:
  kryo.setRegistrationRequired(false)
  kryo.addDefaultSerializer(classOf[List[AnyRef]], new ScalaListSerializer[AnyRef])
  kryo.register(mf.erasure)
  // Put in some singletons:
  kryo.register(this.getClass, new SingletonSerializer(this))
  kryo.register(None.getClass, new SingletonSerializer(None))
  kryo.register(Nil.getClass, new SingletonSerializer(Nil))

  kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

  // TODO: we can read independently of reading, and writing just appends
  private val file = new RandomAccessFile(filename,"rw")
  // We have to init to avoid writing something in the "null" offset of 0:
  file.writeInt(0x5ca1ab1e)
  private val output = new KOutput(4096, 1 << 20)
  private val emptyPtrNodes = MHash[Short, DiskPtrNode[T]]()

  val LEAFNODE = 1 : Byte
  val PTRNODE = 2 : Byte
  // Minimal amount to read to compute the length to read:
  val headerSize = 5

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
      case LEAFNODE => readLeaf(ptr, buf)
      case PTRNODE => readPtrNode(ptr, buf)
      case _ => error("Unrecognized node type: " + objType)
    }
  }

  protected def readLeaf(ptr : Long, buf : Array[Byte]) : DiskLeaf[T] = {
    val input = new KInput(buf)
    // These bool parameters mean optimize for positive sizes:
    val height = input.readShort(true)
    val pos = input.readLong(true)
    val obj = kryo.readClassAndObject(input).asInstanceOf[T]
    new DiskLeaf[T](ptr, height, pos, obj, this)
  }

  protected def readPtrNode(ptr : Long, buf : Array[Byte]) : DiskPtrNode[T] = {
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

  override def empty(height : Short) : PtrNode[T,Long] = {
    emptyPtrNodes.synchronized {
      emptyPtrNodes.getOrElseUpdate(height,
        allocPtrNode(0L, height, Block.alloc[Long])
      )
    }
  }

  override def ptrOf(node : Node[T,Long]) = {
    node match {
      case leaf : DiskLeaf[_] => leaf.ptr
      case ptrNode : DiskPtrNode[_] => ptrNode.ptr
      case _ => error("Invalid node")
    }
  }

  protected def writeBytes(objType : Byte, toWrite : Array[Byte]) : Long = {
    file.synchronized {
      val thisPtr = file.getFilePointer
      file.writeByte(objType)
      file.writeInt(toWrite.length)
      file.write(toWrite)
      thisPtr
    }
  }

  override def allocLeaf(height : Short, pos : Long, value : T) = {
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
  override def allocPtrNode(sz : Long, height : Short, ptrs : Block[Long]) = {
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

class DiskLeaf[T](val ptr : Long, hs : Short, ps : Long, v : T, m : Allocator[T,Long]) extends
  Leaf[T,Long](hs, ps, v, m)

class DiskPtrNode[T](val ptr : Long, sz : Long, height : Short, ptrs : Block[Long],
  mem : Allocator[T,Long]) extends PtrNode[T,Long](sz, height, ptrs, mem)
