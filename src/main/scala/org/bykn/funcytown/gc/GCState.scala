package org.bykn.funcytown.gc

import scala.collection.immutable.{List => sciList, Queue}

import org.bykn.funcytown.Allocator

object GCState {
  def init(alloc : Allocator[Long], memReg : (Long) => ContiguousRegion, minPtr: Long) : GCState = {
    new GCState(alloc, memReg, minPtr, Set[Long](), EmptySpace, minPtr, minPtr, Set[Long](), None)
  }
}

case class GCState(alloc : Allocator[Long],
  memRegionOf : (Long) => ContiguousRegion,
  minPtr : Long,
  pinned : Set[Long],
  freeSpace : MemoryMap,
  maxPtr: Long, lastGCPtr : Long,
  lastPinnedSearch : Set[Long],
  runningGC : Option[MemorySearch]) {

  // Run if we just freed, and we haven't run in a while
  protected def timeToRun(gcIntervalBytes : Long) : Boolean = {
    maxPtr > (lastGCPtr + gcIntervalBytes)
  }

  protected lazy val calculateFreeSpace : Option[UnallocatedSpace] = {
    // If it wasn't free before, and it is now, we know that no matter what,
    // it must still be free (because the allocator wasn't allocating it
    runningGC.map {
      val pinnedQ = pinned
        .foldLeft(Queue[Long]()) { (old, reg) => old.enqueue(reg) }
      _.breadthFirst(pinnedQ) - UnallocatedSpace(RegionSet(freeSpace))
    }
  }
  lazy val pinnedDiff = pinned diff lastPinnedSearch

  def freedSize : Long = calculateFreeSpace.map { _.size }.getOrElse(0L)

  // This should be running:
  def finishGC(current : GCState) : GCState = {
    calculateFreeSpace.map { ua =>
      val newFreeSpace = ua.space
        .foldLeft(RegionSet(current.freeSpace)) { (oldf, reg) => oldf + reg }
      current.copy(freeSpace = newFreeSpace,
        lastGCPtr = maxPtr,
        lastPinnedSearch = pinned,
        runningGC = None)

    }.getOrElse {
      //println("Freed: 0")
      current
    }
  }

  protected lazy val memSearch = new MemorySearch(alloc) {
    val empty = UnallocatedSpace.range(minPtr, maxPtr)
    def regionOf(ptr : Long) = memRegionOf(ptr)
  }

  def startGC(gcIntervalBytes : Long, minDiffOfPinnedSet : Int) : GCState = {
    if(runningGC.isDefined) {
      //We only run one at a time:
      this
    }
    else if(!timeToRun(gcIntervalBytes)) {
      // Not enough extra space as been allocated
      this
    }
    else if((!lastPinnedSearch.isEmpty) && pinnedDiff.size < minDiffOfPinnedSet) {
      //Not enough change yet
      this
    }
    else {
      //Time to run
      copy(runningGC = Some(memSearch))
    }
  }
}
