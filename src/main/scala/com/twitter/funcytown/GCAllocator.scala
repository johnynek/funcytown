package com.twitter.funcytown

import java.io.RandomAccessFile

import scala.collection.immutable.{List => sciList}
import scala.collection.immutable.{SortedMap, SortedSet, Queue}
import scala.actors.Actor
import scala.actors.Actor._

import scala.annotation.tailrec

/**
 * Represents free space
 */
abstract class MemoryMap {
  def size : Long
  def alloc(size : Long) : Option[(Long, MemoryMap)]
}

case object EmptySpace extends MemoryMap {
  override def size = 0L
  override def alloc(size : Long) = None
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
}

case class RegionSet(spaces : SortedMap[Int, sciList[ContiguousRegion]]) extends MemoryMap {
  import RegionSet._
  protected def +(reg : ContiguousRegion) : RegionSet = {
    if (reg.size > 0) {
      val bucket = intLog(reg.size)
      val newList = reg :: (spaces.getOrElse(bucket, sciList[ContiguousRegion]()))
      RegionSet(spaces + (bucket -> newList))
    }
    else {
      this
    }
  }

  override def size : Long = {
    spaces.mapValues { _.map { _.size }.sum }.values.sum
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
  def range(lowerBound : Long, strictUpperBound : Long) = {
    val set = SortedSet[ContiguousRegion]()
    UnallocatedSpace(set + ContiguousRegion(lowerBound, strictUpperBound - lowerBound))
  }
  def apply(crs : ContiguousRegion*) = new UnallocatedSpace(SortedSet(crs :_*))
}
// This class is used to efficiently find the set of allocated space
case class UnallocatedSpace(space : SortedSet[ContiguousRegion]) {
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

  //Remove from the pool
  def -(inuse : ContiguousRegion) : UnallocatedSpace = {
    findRegionContaining(inuse.ptr).map { containing =>
      val remaining = (space - containing)
      new UnallocatedSpace(
        (containing - inuse).foldLeft(remaining) { (set, freeRegion) => set + freeRegion }
      )
    }.getOrElse(this)
  }
  def size : Long = space.map { _.size }.sum
  // Convert to RegionSet for efficient allocation
  def toRegionSet : RegionSet = {
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

/*
 * Adds a separate thread to do GC of the files on the disk
 */
trait GCFileStorage extends AsyncWriterStorage with Allocator[Long] {
  private val lock = new Object
  private var pinned = Set[Long]()
  private var freeSpace : MemoryMap = EmptySpace

  private var maxPtr : Long = minPtr
  private var lastGCPtr : Long = minPtr
  private var lastPinnedSearch = Set[Long]()
  private var runningGC : Option[MemorySearch] = None
  // Everytime the file grows by this much, trigger a GC (on free)
  val gcIntervalBytes : Long
  val minDiffOfPinnedSet = 100 // How different must the pinned set be before running GC?

  protected def addToPinned(ptr : Long) : Set[Long] = lock.synchronized {
    pinned = pinned + ptr
    pinned
  }

  // Here we find a space to write into
  override def getPointer(objType : Byte, toWrite : Array[Byte]) : Long = {
    lock.synchronized {
      val size = sizeOf(objType, toWrite)
      freeSpace.alloc(size) match {
        case Some(ptrMmap) => { freeSpace = ptrMmap._2; ptrMmap._1 }
        case None => {
          //Have to allocate at the end:
          val ptr = maxPtr
          maxPtr += size
          ptr
        }
      }
    }
  }

  // Release space to be reallocated
  override def free(ptr : Long) {
    lock.synchronized {
      pinned = pinned - ptr
      // Run if we just freed, and we haven't run in a while
      if (maxPtr > (lastGCPtr + gcIntervalBytes) && (runningGC.isEmpty)) { gcActor ! Run }
    }
  }
  protected def regionOf(ptr : Long) = ContiguousRegion(ptr, sizeOf(ptr))

  // This is blocking and should not be called in the main allocation thread
  protected def gc {
    @tailrec
    def gcInternal(srch : MemorySearch, alreadySearched : SortedSet[Long],
      free : UnallocatedSpace) : Unit = {
      lock.synchronized {
        if (runningGC.map { _ != srch }.getOrElse(false)) {
          // Someone else is running other than us
          None
        }
        else if((pinned diff lastPinnedSearch).size < minDiffOfPinnedSet) {
          //Is the pinned set different enough?
          None
        }
        else {
          val toSearch = pinned -- alreadySearched
          if (toSearch.isEmpty) {
            //We are done, inside the lock update
            println("old space: " + freeSpace.size)
            freeSpace = free.toRegionSet
            println("new space: " + freeSpace.size)
            lastGCPtr = srch.thisMax
            lastPinnedSearch = pinned
            runningGC = None
            None
          }
          else {
            //Return the ones we haven't yet searched:
            runningGC = Some(srch)
            Some(toSearch)
          }
        }
      }
      .map { difference =>
        //Which ones did we overlook
        (alreadySearched ++ difference,
          srch.breadthFirst( Queue(difference.toSeq.map { regionOf } : _*), free))
      } match {
        case Some((newSearched, newFree)) => gcInternal(srch, newSearched, newFree)
        case None => ()
      }
    }
    try {
      // Note we should not be holding the lock during this operation:
      val search = MemorySearch(lock.synchronized { maxPtr })
      // Initially we haven't searched anything
      gcInternal(search, SortedSet[Long](), search.empty)
    }
    catch {
      // If we have any failure, reset the flag
      case x : Throwable => {
        lock.synchronized { runningGC = None }
        throw x
      }
    }
  }

  case class MemorySearch(val thisMax : Long) extends
    GraphSearch[ContiguousRegion, UnallocatedSpace] {
    val empty = UnallocatedSpace.range(minPtr, thisMax)
    def visit(node : ContiguousRegion, visited : UnallocatedSpace) = visited - node
    def haveVisited(node : ContiguousRegion, visited : UnallocatedSpace) = {
      // We remove the region when we visit
      visited.contains(node)
    }
    def neighborsOf(node : ContiguousRegion) = {
      deref(node.ptr)
        .asInstanceOf[ImmutableDagNode[Long]]
        .pointers
        .map { ptr => regionOf(ptr) }
    }
  }

  case object Run
  override def init = { super.init; gcActor.start }
  override def stop { super.stop; gcActor ! Stop }

  protected val gcActor = new Actor {
    def act() { loop { react {
      case Run => gc
      case Stop => exit
    } } }
  }
}

class GCDiskAllocator(cachedItems : Int, filename : String = null)
  extends CachingByteAllocatorBase(cachedItems) with GCFileStorage {

  override val gcIntervalBytes = 1L << 20 // 1 MiB
  // TODO check file.deleteOnExit() to make sure this gets cleaned up
  private val realFileName = Option(filename).getOrElse(java.util.UUID.randomUUID.toString)
  override val file = new RandomAccessFile(realFileName,"rw")
  // initialize the file, start the write thread
  this.init

  override def afterAlloc[T](ptr : Long, obj : T) : T = {
    // When we alloc we pin, when we free, we unpin
    addToPinned(ptr)
    super.afterAlloc(ptr, obj)
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
