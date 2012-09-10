package org.bykn.funcytown

import java.io.RandomAccessFile

import org.bykn.funcytown.io._

import scala.collection.SeqView
import scala.collection.immutable.{List => sciList}
import scala.collection.immutable.{SortedMap, SortedSet, Queue}
import scala.actors.Actor
import scala.actors.Actor._

import scala.annotation.tailrec

/**
 * Represents free space
 */
sealed abstract class MemoryMap {
  def size : Long
  def alloc(size : Long) : Option[(Long, MemoryMap)]
}

case object EmptySpace extends MemoryMap {
  def size = 0L
  def alloc(size : Long) = None
}

case class ContiguousRegion(ptr : Long, override val size : Long) extends MemoryMap
  with Ordered[ContiguousRegion] {
  override def alloc(space : Long) = {
    if(space < size) {
      //We can allocate
      Some((ptr, ContiguousRegion(ptr + space, size - space)))
    }
    else if(space > size) {
      //Can't fit
      None
    }
    else {
      Some((ptr, EmptySpace))
    }
  }
  def compare(that : ContiguousRegion) = {
    if( ptr != that.ptr) {
      ptr.compare(that.ptr)
    }
    else {
      size.compare(that.size)
    }
  }
  def containsPtr(thatPtr : Long) : Boolean = {
    (ptr <= thatPtr) && (thatPtr < (ptr + size))
  }
  def containsPtrOpt(thatPtr : Long) = if (containsPtr(thatPtr)) Some(this) else None

  def contains(cr : ContiguousRegion) : Boolean = {
    containsPtr(cr.ptr) && containsPtr(cr.ptr + cr.size - 1L)
  }
  def containsOpt(cr : ContiguousRegion) = if (contains(cr)) Some(this) else None

  def -(cr : ContiguousRegion) : sciList[ContiguousRegion] = {
    assert(contains(cr), this.toString + " doesn't contain " + cr.toString)
    val headSize = cr.ptr - ptr
    if(headSize > 0) {
      //Peel off the front, which hasn't actually changed the size head
      ContiguousRegion(ptr, headSize) ::
        //Recurse
        (ContiguousRegion(cr.ptr, size - headSize) - cr)
    }
    else {
      // must have ptr == cr.ptr
      if (size > cr.size) {
        sciList(ContiguousRegion(cr.ptr + cr.size, size - cr.size))
      }
      else {
        // We are taking the whole thing
        Nil
      }
    }
  }
}

object RegionSet {
  val logBase = 1.5
  val empty = RegionSet(SortedMap[Int, sciList[ContiguousRegion]]())
  def intLog(sz : Long) = (scala.math.log(sz)/scala.math.log(logBase)).toInt + 1
  def apply(m : MemoryMap) : RegionSet = {
    m match {
      case EmptySpace => empty
      case c : ContiguousRegion => empty + c
      case r : RegionSet => r
    }
  }
}

case class RegionSet(spaces : SortedMap[Int, sciList[ContiguousRegion]]) extends MemoryMap {
  import RegionSet._
  def +(reg : ContiguousRegion) : RegionSet = {
    if (reg.size > 0) {
      val bucket = intLog(reg.size)
      //TODO Prefer lower pointers
      val newList = (reg :: (spaces.getOrElse(bucket, sciList[ContiguousRegion]())))
      RegionSet(spaces + (bucket -> newList))
    }
    else {
      this
    }
  }

  override lazy val size : Long = {
    spaces.mapValues { _.map { _.size }.sum }.values.sum
  }

  def regions : Iterator[ContiguousRegion] = {
    spaces.iterator.flatMap { kv => kv._2.iterator }
  }

  protected def -(reg : ContiguousRegion) : RegionSet = {
    val bucket = intLog(reg.size)
    val newList = spaces(bucket).filter { _ != reg }
    if (newList.isEmpty) {
      RegionSet(spaces - bucket)
    }
    else {
      RegionSet(spaces + (bucket -> newList))
    }
  }

  @tailrec
  private def allocateFrom(space : Long, from : SortedMap[Int, sciList[ContiguousRegion]]) :
    Option[(Long, MemoryMap)] = {
    if (from.isEmpty) { None }
    else {
      //Try to find on in this list:
      val (thisBucket, thisList) = from.head
      val result = thisList
        .find { _.size >= space }
        .map { cr =>
          // Pull off the front:
          val ptr = cr.ptr
          val newMMap = (this - cr) + ContiguousRegion(cr.ptr + space, cr.size - space)
          (ptr, newMMap)
        }
      if (result.isDefined) {
        result
      }
      else {
        allocateFrom(space, from.tail)
      }
    }
  }
  // Check all the buckets large enough to hold this memory
  override def alloc(space : Long) = allocateFrom(space, spaces.rangeImpl(Some(intLog(space)), None))
}

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

abstract class GraphSearch[N,V] {
  def empty : V
  def visit(node: N, visited : V) : V
  def haveVisited(node : N, visited : V) : Boolean
  def neighborsOf(from : N) : Iterable[N]

  def getReachable(from : N) : V = breadthFirst(Queue(from), empty)

  @tailrec
  final def breadthFirst(toVisit : Queue[N], visited : V = empty, toVisitSet : Set[N] = null) : V = {
    if (toVisit.isEmpty) {
      visited
    }
    else {
      val inToVisit = Option(toVisitSet).getOrElse(toVisit.toSet)
      val (next, rest) = toVisit.dequeue
      val newVisited = visit(next, visited)
      val newNodes = neighborsOf(next).filter { node =>
        !haveVisited(node, newVisited) && !inToVisit(node)
      }
      breadthFirst(rest ++ newNodes, newVisited, inToVisit ++ newNodes)
    }
  }
}

private[funcytown] abstract class MemorySearch(alloc : Allocator[Long]) extends
  GraphSearch[Long, UnallocatedSpace] {
  def visit(node : Long, visited : UnallocatedSpace) = visited - regionOf(node)
  def haveVisited(node : Long, visited : UnallocatedSpace) = {
    // We remove the region when we visit
    visited.findRegionContaining(node).isEmpty
  }
  def regionOf(ptr : Long) : ContiguousRegion
  def neighborsOf(ptr : Long) = {
    alloc.deref[ImmutableDagNode[Long]](ptr).pointers
  }
}

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
