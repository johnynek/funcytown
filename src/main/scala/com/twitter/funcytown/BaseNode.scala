package com.twitter.funcytown

import scala.annotation.tailrec

object Allocator extends LowPriorityAllocator {
  // TODO add disk allocators
}

trait Allocator[T, PtrT] {
  val nullPtr : PtrT
  def deref(ptr : PtrT) : Node[T,PtrT]
  def ptrOf(node : Node[T,PtrT]) : PtrT
  def empty(height : Short)(implicit mf : Manifest[PtrT]) : PtrNode[T,PtrT] = {
    allocPtrNode(0L, height, Block.alloc[PtrT])
  }
  def allocLeaf(height : Short, pos : Long, value : T) : Leaf[T,PtrT]
  def allocPtrNode(sz : Long, height : Short, ptrs : Block[PtrT]) : PtrNode[T,PtrT]
}

abstract class Node[T,PtrT] {
  def apply(pos : Long) = get(pos).get
  def get(pos : Long) : Option[T] = findLeaf(pos).map { _.value }
  def isEmpty : Boolean = (size == 0)
  def findLeaf(pos : Long) : Option[Leaf[T, PtrT]]
  def put(pos : Long, value : T) : Node[T, PtrT] = {
    // Just replace:
    map(pos) { x => Some(value) }._2
  }
  def take(pos : Long) : (Option[T], Node[T, PtrT]) = {
    // Just erase whatever is there:
    map(pos) { old => None }
  }
  def size : Long
  def map(pos : Long)(fn : Option[T] => Option[T]) : (Option[T], Node[T,PtrT])
  def toStream : Stream[T]
}

class PtrNode[T, PtrT](sz : Long, val height : Short, val ptrs : Block[PtrT],
  mem : Allocator[T,PtrT])(implicit mf : Manifest[PtrT]) extends Node[T,PtrT] {
  override def findLeaf(pos : Long) : Option[Leaf[T,PtrT]] = {
    val (thisIdx, nextPos) = Block.toBlockIdx(height, pos)
    val nextNode = mem.deref(ptrs(thisIdx))
    if (null != nextNode) {
      nextNode.findLeaf(nextPos)
    }
    else {
      None
    }
  }

  override def map(pos : Long)(fn : Option[T] => Option[T]) : (Option[T], Node[T,PtrT]) = {
    if (pos < maxPos) {
      // This position cannot possibly be in the tree
      val (thisIdx, nextPos) = Block.toBlockIdx(height, pos)
      val nextPtr = ptrs(thisIdx)
      val (oldVal, newPtr, szdelta) = if (mem.nullPtr == nextPtr) {
        val value = fn(None)
        if (value.isDefined) {
          // This is a new value:
          (None, mem.ptrOf(mem.allocLeaf(height, nextPos, value.get)), 1L)
        }
        else {
          // Mapping None => None, weird, but okay.
          (None, mem.nullPtr, 0L)
        }
      }
      else {
        val nextNode = mem.deref(nextPtr)
        // Replace down the tree:
        val (old, resNode) = nextNode.map(nextPos)(fn)
        (old, mem.ptrOf(resNode), resNode.size - nextNode.size)
      }
      (oldVal, mem.allocPtrNode(sz + szdelta, height, ptrs.updated(thisIdx, newPtr)))
    }
    else {
      val newValue = fn(None)
      if (newValue.isDefined) {
        //We need a level above us:
        val newBlock = Block.alloc[PtrT].updated(0, mem.ptrOf(this))
        mem.allocPtrNode(sz, (height + 1).toShort, newBlock).map(pos)(fn)
      }
      else {
        //None => None
        (None, this)
      }
    }
  }

  private def maxPos : Long = {
    // This is 2^(7*(height + 1))
    1 << (Block.SHIFT * (height + 1))
  }

  override def size = sz

  override def toStream : Stream[T] = {
    // Just get the children streams out in order:
    ptrs.foldLeft(Stream.empty[T]) { (oldStream, newPtr) =>
      if (newPtr != mem.nullPtr) {
        oldStream ++ (mem.deref(newPtr).toStream)
      }
      else {
        oldStream
      }
    }
  }
}

class Leaf[T,PtrT](val height : Short, val pos : Long, val value : T, mem : Allocator[T,PtrT])
  (implicit mf : Manifest[PtrT])
  extends Node[T,PtrT] {
  override def isEmpty = false
  override def findLeaf(inpos : Long) = {
    if (pos == inpos) {
      Some(this)
    }
    else {
      None
    }
  }

  override def map(inpos : Long)(fn : Option[T] => Option[T]) : (Option[T], Node[T, PtrT]) = {
    val oldValue = Some(value)
    if (pos == inpos) {
      val newValue = fn(oldValue)
      if (newValue.isDefined) {
        // Replace:
        (oldValue, mem.allocLeaf(height, pos, newValue.get))
      }
      else {
        // This node is now deleted:
        (oldValue, mem.empty(height))
      }
    }
    else {
      //We have to deepen the tree here:
      val newValue = fn(None)
      if (newValue.isDefined) {
        assert(height > 0)
        val resNode = mem.empty(height)
          .put(pos, value)
          .put(inpos, newValue.get)
        (None, resNode)
      }
      else {
        // None => None case, so do nothing:
        (None, this)
      }
    }
  }
  override def size = 1L
  override def toStream = Stream(value)
}
