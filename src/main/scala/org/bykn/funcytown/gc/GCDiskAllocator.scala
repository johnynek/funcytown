package org.bykn.funcytown.gc

import org.bykn.funcytown.disk._
import org.bykn.funcytown.io._
import org.bykn.funcytown.SyncVar

import scala.collection.SeqView
import scala.collection.immutable.{List => sciList, Queue}
import scala.actors.Actor
import scala.actors.Actor._

import scala.annotation.tailrec

class GCDiskAllocator(spillSize : Int = 10000000, filename : String = null)
  extends ByteAllocator with GCFileStorage {

  override val gcIntervalBytes = 1L << 18 // 256k
  // TODO check file.deleteOnExit() to make sure this gets cleaned up
  private val realFileName = Option(filename).getOrElse(java.util.UUID.randomUUID.toString)
  // initialize the file, start the write thread
  override val file = new SyncVar[FileLike](FileLikeSeq(MemoryFileLike(spillSize), RandAccessFileLike(realFileName)))
  this.init

  override def afterAlloc[T](ptr : Long, obj : T) : T = {
    // When we alloc we pin, when we free, we unpin
    addToPinned(ptr)
    super.afterAlloc(ptr, obj)
  }

  override def deref[T](ptr : Long) = {
    // This is pinned again:
    try {
      addToPinned(ptr)
      super.deref[T](ptr)
    }
    catch {
      case t : Throwable => {
        free(ptr)
        throw new Exception(t)
      }
    }
  }

  override def finalize { close }

  override def close {
    super.close
    // Delete the file if we need to:
    if (filename == null) {
      (new java.io.File(realFileName)).delete
    }
  }
}
