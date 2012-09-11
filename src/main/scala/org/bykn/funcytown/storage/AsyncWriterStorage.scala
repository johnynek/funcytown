package org.bykn.funcytown.storage

import org.bykn.funcytown._

import scala.actors.Actor
import scala.actors.Actor._

trait AsyncWriterStorage extends FileStorage {
  case class WriteRecord(pos : Long)

  private val writeCache = new SyncVar(Map[Long, (Byte, Array[Byte])]())

  private case object Stop

  protected val writingActor = new Actor {

    def act() {
      loop {
        react {
          case WriteRecord(pos) => {
            // This is the record we just wrote:
            writeCache.mutate { wc =>
              wc.get(pos)
                // Make sure what we just wrote is the latest
                .map { objBytes =>
                  writeAt(pos, objBytes._1, objBytes._2)
                  // Now we have written, remove from the cache:
                  wc - pos
                }
                .getOrElse(wc)
            }
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
    writeCache.get.get(ptr)
      .map { _._2.size + 1 }
      // Else we have to read off disk
      .getOrElse { super.sizeOf(ptr) }
  }
  // There is a race if we don't cache the data until it is written
  override def readBytes(ptr : Long) : (Byte, Array[Byte]) = {
    writeCache.get.get(ptr)
      .getOrElse { super.readBytes(ptr) }
  }
  override def writeBytes(objType : Byte, toWrite : Array[Byte]) : Long = {
    val ptr = getPointer(objType, toWrite)
    writeCache.mutate { _ + (ptr -> (objType, toWrite)) }
    writingActor ! WriteRecord(ptr)
    ptr
  }
}
