package org.bykn.funcytown.io

import java.io.RandomAccessFile

object RandAccessFileLike {
  def apply(name : String) = new RandAccessFileLike(0L, new RandomAccessFile(name, "rw"))
}

class RandAccessFileLike(override val position : Long, file : RandomAccessFile) extends FileLike {
  override def close = { file.close; this }
  override def toHead = new RandAccessFileLike(0L, file)
  // return how much to the right we moved and a new FileLike
  override def moveRight(right : Long) = {
    val newPos = position + right
    //Make sure we can move there:
    file.seek(newPos)
    (right, new RandAccessFileLike(newPos, file))
  }
  override def read(into : Array[Byte], offset : Int, size : Int) = {
    file.seek(position)
    val rsize = file.read(into, offset, size)
    if(rsize > 0) {
      //We succesfully read
      (rsize.toLong, new RandAccessFileLike(position + rsize, file))
    }
    else {
      // Could not read at all
      (0L, this)
    }
  }
  override def write(from : Array[Byte], offset : Int, size : Int) = {
    file.seek(position)
    file.write(from, offset, size)
    (size.toLong, new RandAccessFileLike(position + size, file))
  }
}

