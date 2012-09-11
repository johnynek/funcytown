package org.bykn.funcytown.storage

trait ByteStorage {
  // read the data at pos and return the header byte, and the header byte, and bytearray
  def readBytes(pos : Long) : (Byte, Array[Byte])
  // the inverse of the above
  def writeBytes(header : Byte, toWrite : Array[Byte]) : Long
}
