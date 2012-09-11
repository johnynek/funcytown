package org.bykn.funcytown.gc

import scala.collection.immutable.{List => sciList, SortedSet, SortedMap}

object UnallocatedSpace {
  val empty = new UnallocatedSpace(SortedSet[ContiguousRegion]())
  def range(lowerBound : Long, strictUpperBound : Long) = {
    empty + ContiguousRegion(lowerBound, strictUpperBound - lowerBound)
  }
  def apply(crs : ContiguousRegion*) = new UnallocatedSpace(SortedSet(crs :_*))
  def apply(rs : RegionSet) = {
    rs.regions.foldLeft(empty) { (uas, c) => uas + c }
  }
}
// This class is used to efficiently find the set of allocated space
case class UnallocatedSpace(val space : SortedSet[ContiguousRegion]) {
  def findRegionContaining(ptr : Long) : Option[ContiguousRegion] = {
    //Create a minimal region with this ptr:
    val minCR = ContiguousRegion(ptr, -1L)
    // The region either starts with this pointer, or this is an upperbound:
    def notEmptyOpt[V]( x : SortedSet[V] ) = { if (!x.isEmpty) Some(x) else None }

    notEmptyOpt(space.rangeImpl(None, Some(minCR)))
      .flatMap { _.max.containsPtrOpt(ptr) }
      .orElse {
        notEmptyOpt(space.rangeImpl(Some(minCR), None))
          .flatMap { _.min.containsPtrOpt(ptr) }
      }
  }
  def contains(reg : ContiguousRegion) : Boolean = {
    findRegionContaining(reg.ptr)
      .map { _.contains(reg) }
      .getOrElse(false)
  }
  //return freed space:
  def +(free : ContiguousRegion) : UnallocatedSpace = {
    UnallocatedSpace(space + free)
  }
  //Remove from the pool
  def -(inuse : ContiguousRegion) : UnallocatedSpace = {
    findRegionContaining(inuse.ptr).map { containing =>
      val remaining = (space - containing)
      // Return any space at the end which is not inuse
      UnallocatedSpace(
        (containing - inuse).foldLeft(remaining) { (set, freeRegion) => set + freeRegion }
      )
    }.getOrElse(this)
  }
  //Remove all the current items that are present in the other:
  def -(that : UnallocatedSpace) : UnallocatedSpace = {
    that.space.foldLeft(this) { (oldspace, reg) => oldspace - reg }
  }

  lazy val size : Long = space.map { _.size }.sum
  // Convert to RegionSet for efficient allocation
  lazy val toRegionSet : RegionSet = {
    RegionSet(
      SortedMap(
        space
          .groupBy { cr => RegionSet.intLog(cr.size) }
          .mapValues { _.toList }
          .toSeq :_*
      )
    )
  }
}
