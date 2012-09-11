package org.bykn.funcytown.gc

import org.bykn.funcytown.disk.ByteAllocator
import org.bykn.funcytown.io._
import org.bykn.funcytown.storage.AsyncWriterStorage
import org.bykn.funcytown.SyncVar

import scala.collection.SeqView
import scala.collection.immutable.{List => sciList, Queue}
import scala.actors.Actor
import scala.actors.Actor._

import scala.annotation.tailrec

class GCDiskAllocator(spillSize : Int = 10000000, filename : String = null)
  extends ByteAllocator with AsyncWriterStorage {

  private val gcState = new SyncVar(GCState.init(this, this.regionOf _, minPtr))

  // Everytime the file grows by this much, trigger a GC (on free)
  val gcIntervalBytes = 1L << 18 // 256kb

  // TODO check file.deleteOnExit() to make sure this gets cleaned up
  private val realFileName = Option(filename).getOrElse(java.util.UUID.randomUUID.toString)
  // initialize the file, start the write thread
  override val file = new SyncVar[FileLike](FileLikeSeq(MemoryFileLike(spillSize), RandAccessFileLike(realFileName)))
  this.init

  protected def addToPinned(ptr : Long) : Set[Long] = {
    gcState.mutateIdem { gcs =>
      gcs.copy(pinned = gcs.pinned + ptr)
    }.pinned
  }

  // Here we find a space to write into
  override def getPointer(objType : Byte, toWrite : Array[Byte]) : Long = {
    val size = sizeOf(objType, toWrite)
    gcState.effectIdem { _.alloc(size) }
  }

  // Release space to be reallocated
  override def free(ptr : Long) {
    gcState.effectIdem { gcs =>
      val state1 = gcs.free(ptr)
      val gc = state1.startGC(gcIntervalBytes)
      (gc, if(gc != state1) Some(gc) else None)
    }.map { gc => gcActor ! gc } //Tell the actor we started a GC
  }
  protected def regionOf(ptr : Long) = ContiguousRegion(ptr, sizeOf(ptr))

  // An inner class to use for messaging:
  private case object Stop
  override def stop { super.stop; gcActor ! Stop }

  protected lazy val gcActor = { val act = new Actor {
      def act() { loop { react {
        case running : GCState => {
          // Now we are definitely running, and try to finish. This can't be combined with
          // the above because then we would rerun the entire search for each retry
          println("GC")
          running.run //Force the evaluation outside of the mutateIdem
          gcState.mutateIdem { gcs => running.finishGC(gcs) }
          println("Freed: " + running.freedSize)
        }
        case Stop => exit
      } } }
    }
    act.start
    act
  }

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
