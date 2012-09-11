package org.bykn.funcytown.gc

import scala.annotation.tailrec
import scala.collection.immutable.{List => sciList, SortedMap}

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
