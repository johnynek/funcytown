package org.bykn.funcytown

import scala.collection.immutable.{List => sciList}

/**
 * This class is the underlying data structure behind the HashMap
 * (and also in principle an persistent vector class)
 * At each position it holds a pointer to some object.
 */
trait Node[PtrT] extends Iterable[PtrT] {
  def allocator : Allocator[PtrT]
  // returns allocator.nullPtr if pos is absent
  def find(pos : Long) : PtrT
  def map(pos : Long)(fn : PtrT => PtrT) : (PtrT, Node[PtrT])

  // These are in terms of the above
  def apply(pos : Long) = get(pos).get
  def get(pos : Long) : Option[PtrT] = allocator.optionPtr(find(pos))
  def put(pos : Long, value : PtrT) : Node[PtrT] = {
    // Just replace:
    map(pos) { x => value }._2
  }
  def take(pos : Long) : (PtrT, Node[PtrT]) = {
    // Just erase whatever is there:
    map(pos) { old => allocator.nullPtr }
  }
}

class PtrNode[PtrT](val height : Short, val ptrs : Block[PtrT],
  mem : Allocator[PtrT])(implicit mf : Manifest[PtrT]) extends Node[PtrT] {
  override def allocator = mem
  override def find(pos : Long) : PtrT = {
    val (thisIdx, nextPos) = Block.toBlockIdx(height, pos)
    mem.optionPtr(ptrs(thisIdx))
      .map { ptr =>
        if(height == 0) {
          ptr
        }
        else {
          mem.deref[Node[PtrT]](ptr).find(pos)
        }
      }
      .getOrElse(mem.nullPtr)
  }

  private lazy val thisPtr = mem.ptrOf(this)

  def replaceAt(idx : Int, ptr : PtrT) : PtrNode[PtrT] = {
    val curPtr = ptrs(idx)
    if (curPtr != ptr) {
      mem.allocPtrNode(height, ptrs.updated(idx, ptr))
    }
    else {
      this
    }
  }

  override def map(pos : Long)(fn : PtrT => PtrT) : (PtrT, Node[PtrT]) = {
    val (thisIdx, nextPos) = Block.toBlockIdx(height, pos)
    if(nextPos == 0L) {
      //We or our child handles it
      val init = ptrs(thisIdx)
      if(height == 0) {
        // We handle it:
        val newNode = replaceAt(thisIdx, fn(init))
        (init, newNode)
      }
      else {
        val childH = height - 1
        val maskedPos = Block.fixHeight(childH, pos)
        val (old, newChild) = mem.derefOpt[Node[PtrT]](init)
          .orElse(Some(mem.empty(childH.toShort)))
          .map { _.map(maskedPos)(fn) }.get
        // Check to see if the newChild has some non-zero blocks
        if (!newChild.isEmpty) {
          (old, replaceAt(thisIdx, mem.ptrOf(newChild)))
        }
        else {
          (old, replaceAt(thisIdx, mem.nullPtr))
        }
      }
    }
    else {
      //We have to insert a new level above us, and the item is not here:
      val newHeight = (height + 1).toShort
      val above = mem.allocPtrNode(newHeight, mem.empty(newHeight).ptrs.updated(0, thisPtr))
      above.map(pos)(fn)
    }
  }

  override def iterator : Iterator[PtrT] = {
    val ptrlistit = ptrs.iterator.filter { _ != mem.nullPtr }

    if (height == 0) {
      ptrlistit
    }
    else {
      ptrlistit.flatMap { ptr => mem.deref[Node[PtrT]](ptr).iterator }
    }
  }
  override def toString = {
    sciList("PtrNode(", height, ptrs, ")").mkString(", ")
  }
}
