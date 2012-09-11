package org.bykn.funcytown.io

object MemoryFileLike {
  def apply(size : Int) = new MemoryFileLike(0L, 0, new Array[Byte](size))
}

// TODO make memory lazy, so we can avoid allocation until really needed
class MemoryFileLike(override val position : Long, max : Int, memory : Array[Byte]) extends FileLike {
  protected def size = memory.size
  override def toHead = new MemoryFileLike(0L, max, memory)
  override def moveRight(right : Long) = {
    val posI = position.toInt
    val available = max - posI
    val rsize = scala.math.min(right, available.toLong)
    (rsize, new MemoryFileLike(position + rsize, max, memory))
  }
  override def read(into : Array[Byte], offset : Int, size : Int) = {
    val posI = position.toInt
    val available = max - posI
    val rsize = scala.math.min(size, available)
    Array.copy(memory, posI, into, offset, rsize)
    (rsize.toLong, new MemoryFileLike(position + rsize, max, memory))
  }
  override def write(from : Array[Byte], offset : Int, size : Int) = {
    val posI = position.toInt
    val available = memory.size - posI
    val wsize = scala.math.min(size, available)
    Array.copy(from, offset, memory, posI, wsize)
    val newMax = scala.math.max(max, posI + wsize)
    (wsize.toLong, new MemoryFileLike(posI + wsize, newMax, memory))
  }
}

