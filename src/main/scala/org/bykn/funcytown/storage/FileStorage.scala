package org.bykn.funcytown.storage

import org.bykn.funcytown.io.FileLike
import org.bykn.funcytown.SyncVar

import scala.annotation.tailrec

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
        Thread.sleep(1) //Sleep 1 ms and try again:
        println("sleep")
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
    eofPtr.effectIdem { eof =>
      // update the pointer, and read it out
      (eof + sizeOf(objType, toWrite), eof)
    }
  }

  def writeAt(pos : Long, objType : Byte, toWrite : Array[Byte]) {
    // We need to hold a lock across this call
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
