package org.bykn.funcytown.gc

import org.bykn.funcytown.Allocator
import org.bykn.funcytown.SyncVar
import org.bykn.funcytown.storage.AsyncWriterStorage

import scala.actors.Actor
import scala.actors.Actor._
/*
 * Adds a separate thread to do GC of the files on the disk
 */
trait GCFileStorage extends AsyncWriterStorage with Allocator[Long] {
//trait GCFileStorage extends Allocator[Long] with FileStorage {
  private val gcState = new SyncVar(GCState.init(this, this.regionOf _, minPtr))

  // Everytime the file grows by this much, trigger a GC (on free)
  val gcIntervalBytes : Long
  val minDiffOfPinnedSet = 1000 // How different must the pinned set be before running GC?

  protected def addToPinned(ptr : Long) : Set[Long] = {
    gcState.mutateIdem { gcs =>
      gcs.copy(pinned = gcs.pinned + ptr)
    }.pinned
  }

  // Here we find a space to write into
  override def getPointer(objType : Byte, toWrite : Array[Byte]) : Long = {
    val size = sizeOf(objType, toWrite)
    gcState.effectIdem { gcs =>
      gcs.freeSpace.alloc(size) match {
        case Some(ptrMmap) => {
          (gcs.copy(freeSpace = ptrMmap._2), ptrMmap._1)
        }
        case None => {
          //Have to allocate at the end:
          (gcs.copy(maxPtr = (gcs.maxPtr + size)), gcs.maxPtr)
        }
      }
    }
  }

  // Release space to be reallocated
  override def free(ptr : Long) {
    gcState.effectIdem { gcs =>
      val state1 = gcs.copy(pinned = gcs.pinned - ptr)
      val gc = state1.startGC(gcIntervalBytes, minDiffOfPinnedSet)
      (gc, if(gc != state1) Some(gc) else None)
    }.map { gc => gcActor ! gc } //Tell the actor we started a GC
  }
  protected def regionOf(ptr : Long) = ContiguousRegion(ptr, sizeOf(ptr))

  override def init = { super.init; gcActor.start }
  // An inner class to use for messaging:
  private case object Stop
  override def stop { super.stop; gcActor ! Stop }

  protected val gcActor = new Actor {
    def act() { loop { react {
      case running : GCState => {
        // Now we are definitely running, and try to finish. This can't be combined with
        // the above because then we would rerun the entire search for each retry
        println("GC")
        gcState.mutateIdem { gcs => running.finishGC(gcs) }
        println("Freed: " + running.freedSize)
      }
      case Stop => exit
    } } }
  }
}

