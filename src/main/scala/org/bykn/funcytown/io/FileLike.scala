package org.bykn.funcytown.io

import scala.annotation.tailrec

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
