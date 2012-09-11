package org.bykn.funcytown.gc

import scala.collection.immutable.{List => sciList, Queue}

import org.bykn.funcytown.Allocator

object GCState {
  def init(alloc : Allocator[Long], memReg : (Long) => ContiguousRegion, minPtr: Long) : GCState = {
    new GCState(alloc, memReg, minPtr, Set[Long](), EmptySpace,
      minPtr, None, 0L, 0L, 0L)
  }
}

/**
 * Immutable state of the Garbage Collector.
 * The methods only compute new states, the states themselves never change.
 */
case class GCState(alloc : Allocator[Long],
  memRegionOf : (Long) => ContiguousRegion,
  minPtr : Long,
  pinned : Set[Long],
  freeSpace : MemoryMap,
  maxPtr: Long,
  runningGC : Option[MemorySearch],
  allocatedSpace : Long,
  lastFreedSpace : Long,
  allocatedAtLastGC : Long
  ) {

  def alloc(size : Long) : (GCState, Long) = {
    freeSpace.alloc(size) match {
      case Some(ptrMmap) => {
        (copy(freeSpace = ptrMmap._2,
              allocatedSpace = allocatedSpace + size), ptrMmap._1)
      }
      case None => {
        //Have to allocate at the end:
        (copy(maxPtr = (maxPtr + size),
              allocatedSpace = allocatedSpace + size), maxPtr)
      }
    }
  }

  def free(ptr : Long) : GCState = copy(pinned = pinned - ptr)

  protected lazy val calculateFreeSpace : Option[UnallocatedSpace] = {
    // If it wasn't free before, and it is now, we know that no matter what,
    // it must still be free (because the allocator wasn't allocating it
    runningGC.map {
      val pinnedQ = pinned
        .foldLeft(Queue[Long]()) { (old, reg) => old.enqueue(reg) }
      _.breadthFirst(pinnedQ) - UnallocatedSpace(RegionSet(freeSpace))
    }
  }


  lazy val expectedFree : Long = {
    //Last time we freed:
    val lastFreedPercentage : Double = lastFreedSpace.toDouble / (allocatedAtLastGC + 1L)
    (lastFreedPercentage * allocatedSpace).toLong
  }

  lazy val freedSize : Long = calculateFreeSpace.map { _.size }.getOrElse(0L)

  // This should be running, current is the most up to date.
  def finishGC(current : GCState) : GCState = {
    calculateFreeSpace.map { ua =>
      val newFreeSpace = ua.space
        .foldLeft(RegionSet(current.freeSpace)) { (oldf, reg) => oldf + reg }

      current.copy(freeSpace = newFreeSpace,
        runningGC = None,
        lastFreedSpace = freedSize,
        allocatedSpace = current.allocatedSpace - freedSize,
        allocatedAtLastGC = this.allocatedSpace
        )

    }.getOrElse {
      current
    }
  }

  protected lazy val memSearch = new MemorySearch(alloc) {
    val empty = UnallocatedSpace.range(minPtr, maxPtr)
    def regionOf(ptr : Long) = memRegionOf(ptr)
  }

  // The GC is lazy, this forces us to evaluate the new free space
  def run : Unit = { freedSize /* we need this in finish anyway */ }

  def startGC(gcIntervalBytes : Long) : GCState = {
    if(runningGC.isDefined) {
      //We only run one at a time:
      this
    }
    else if((allocatedSpace - allocatedAtLastGC) < gcIntervalBytes) {
      //We can't possibly free very much
      this
    }
    else if((expectedFree > 0L) && (expectedFree < gcIntervalBytes)) {
      //We can't possibly free very much
      this
    }
    else {
      //Time to run
      copy(runningGC = Some(memSearch))
    }
  }
}
