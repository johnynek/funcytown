package org.bykn.funcytown

import java.io.RandomAccessFile

import scala.annotation.tailrec
import scala.collection.immutable.{List => sciList}

// Abstract memory and file storage in a poor-man's Monad, but not really a monad,
// because these objects are not immutable due to the side-effects of IO
trait FileLike {
  def close : FileLike = { this }
  def position : Long
  // Go back to the start
  def toHead : FileLike
  // return how much to the right we moved and a new FileLike
  def moveRight(pos : Long) : (Long, FileLike)
  // Returns how many bytes were read, and the state of the FileLike
  def read(into : Array[Byte], offset : Int, size : Int) : (Long, FileLike)
  // Return the pair of how much was written, and the resulting writer
  def write(from : Array[Byte], offset : Int, size : Int) : (Long, FileLike)
  // These are in terms of the above:
  def seek(pos : Long) : FileLike = {
    @tailrec
    def _keepRight(fl : FileLike, seekRight : Long) : FileLike = {
      if( seekRight > 0) {
        val (moved, newFl) = fl.moveRight(seekRight)
        _keepRight(newFl, seekRight - moved)
      }
      else {
        fl
      }
    }
    // go the the head, and then keep moving right
    _keepRight(toHead, pos)
  }
  def readByte : (Byte, FileLike) = {
    val ba = new Array[Byte](1)
    val fl = readFully(ba)
    (ba(0), fl)
  }
  def readInt : (Int, FileLike) = {
    def posByte(b : Byte) : Int = {
      val bi = b.toInt
      if( b < 0) { bi + 256 }
      else { bi }
    }
    // TODO: do only one call to readFully here
    (0 to 3).foldLeft((0,this)) { (intFl, idx) =>
      //Big-endian
      val (b, fl) = intFl._2.readByte
      ((intFl._1 << 8) | posByte(b), fl)
    }
  }
  def writeByte(b : Byte) : FileLike = writeFully(Array(b))
  def writeInt(towrite : Int) : FileLike = {
    // TODO: do only one call to writeFully here
    (0 to 3).foldLeft(this) { (fl,idx) =>
      fl.writeByte(((towrite >> ((3 - idx) * 8)) & 0xFF).toByte)
    }
  }
  def readFully(into : Array[Byte]) : FileLike = {
    @tailrec
    def _read(from : FileLike, into : Array[Byte], offset : Int, size : Int) : FileLike = {
      if(size > 0) {
        val (thisRead, nextFL) = from.read(into, offset, size)
        if(thisRead == 0L && nextFL == from) {
          //This has failed:
          throw new java.io.EOFException("Could not read from: " + from.toString)
        }
        val intThisRead = thisRead.toInt
        _read(nextFL, into, offset + intThisRead, size - intThisRead)
      }
      else {
        from
      }
    }
    _read(this, into, 0, into.size)
  }
  def writeFully(from : Array[Byte]) : FileLike = {
    @tailrec
    def _write(to : FileLike, from : Array[Byte], offset : Int, size : Int) : FileLike = {
      if(size > 0) {
        val (thisWrite, nextFL) = to.write(from, offset, size)
        if(thisWrite == 0L && nextFL == from) {
          //This has failed:
          throw new java.io.EOFException("Could not write in: " + from.toString)
        }
        val intThisWrite = thisWrite.toInt
        _write(nextFL, from, offset + intThisWrite, size - intThisWrite)
      }
      else {
        to
      }
    }
    _write(this, from, 0, from.size)
  }
}

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

object MemoryFileLike {
  def apply(size : Int) = new MemoryFileLike(0L, 0, new Array[Byte](size))
}

class MemoryFileLike(override val position : Long, max : Int, memory : Array[Byte]) extends FileLike {
  protected val size = memory.size
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

object FileLikeSeq {
  def apply(filelike : FileLike*) = {
    new FileLikeSeq(sciList(filelike : _*), sciList[FileLike](), 0L)
  }
}

/**
 * This is how we handle spilling: we first put a MemoryFileLike followed
 * by a RandFileLike, so we can go to memory first.  In principle
 * we could layer deeper and deeper if we needed (maybe a file on SSD, then HD, then
 * network disk)
 */
class FileLikeSeq(next : sciList[FileLike], passed : sciList[FileLike],
  override val position : Long) extends FileLike {

  override def close = {
    next.foreach { _.close }
    passed.foreach { _.close }
    this
  }

  override def toHead = FileLikeSeq {
    passed
      //Push all the passed back into next
      .foldLeft(next) { (old, fl) => fl :: old }
      //go back to head
      .map { _.toHead } : _*
    }

  protected def doRight(amt : Long)(action : (FileLike) => (Long, FileLike)) : (Long,FileLike) = {
    next.headOption.map { nextHead =>
      val movedNextFL = action(nextHead)
      val moved = movedNextFL._1
      val (nn, np) = if (moved == amt) {
        // next.head could accomodate
        val newNext = movedNextFL._2 :: next.tail
        (newNext, passed)
      }
      else {
        //We exhausted the head:
        (next.tail, movedNextFL._2 :: passed)
      }
      (moved, new FileLikeSeq(nn, np, position + moved))
    }
    .getOrElse {
      (0L, this)
    }
  }
  override def moveRight(right : Long) = doRight(right) { _.moveRight(right) }
  override def read(into : Array[Byte], offset : Int, size : Int) = doRight(size) {
    _.read(into, offset, size)
  }
  override def write(into : Array[Byte], offset : Int, size : Int) = doRight(size) {
    _.write(into, offset, size)
  }
}

