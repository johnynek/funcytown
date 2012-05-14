package com.twitter.funcytown

import scala.collection.immutable.{List => sciList}

abstract class Node[T,PtrT] {
  def apply(pos : Long) = get(pos).get
  def get(pos : Long) : Option[T] = findLeaf(pos).map { _.value }
  def isEmpty : Boolean
  def findLeaf(pos : Long) : Option[Leaf[T, PtrT]]
  def put(pos : Long, value : T) : Node[T, PtrT] = {
    // Just replace:
    map(pos) { x => Some(value) }._2
  }
  def take(pos : Long) : (Option[T], Node[T, PtrT]) = {
    // Just erase whatever is there:
    map(pos) { old => None }
  }
  def map(pos : Long)(fn : Option[T] => Option[T]) : (Option[T], Node[T,PtrT])
  def toStream : Stream[T]
}

class PtrNode[T, PtrT](val height : Short, val ptrs : Block[PtrT],
  mem : Allocator[PtrT])(implicit mf : Manifest[PtrT]) extends Node[T,PtrT] {
  override def isEmpty : Boolean = {
    ptrs.foldLeft(true) { (empty, ptr) =>
      empty && ((ptr == mem.nullPtr) || mem.deref(ptr).asInstanceOf[Node[T,PtrT]].isEmpty)
    }
  }
  override def findLeaf(pos : Long) : Option[Leaf[T,PtrT]] = {
    val (thisIdx, nextPos) = Block.toBlockIdx(height, pos)
    val nextPtr = ptrs(thisIdx)
    if (mem.nullPtr != nextPtr) {
      // nextPtr has height strictly greater
      val nextNode = mem.deref(nextPtr).asInstanceOf[Node[T,PtrT]]
      nextNode.findLeaf(nextPos)
    }
    else {
      None
    }
  }

  override def map(pos : Long)(fn : Option[T] => Option[T]) : (Option[T], Node[T,PtrT]) = {
    val (thisIdx, nextPos) = Block.toBlockIdx(height, pos)
    val nextPtr = ptrs(thisIdx)
    val (oldVal, newPtr) = if (mem.nullPtr == nextPtr) {
      val value = fn(None)
      if (value.isDefined) {
        // This is a new value:
        (None, mem.ptrOf(mem.allocLeaf((height + 1).toShort, nextPos, value.get)))
      }
      else {
        // Mapping None => None, weird, but okay.
        (None, mem.nullPtr)
      }
    }
    else {
      // nextPtr has height strictly greater
      val nextNode = mem.deref(nextPtr).asInstanceOf[Node[T,PtrT]]
      // Replace down the tree:
      val (old, resNode) = nextNode.map(nextPos)(fn)
      // If it is empty, we don't need to store, it, just put null
      (old, if (resNode.isEmpty) mem.nullPtr else mem.ptrOf(resNode))
    }
    (oldVal, mem.allocPtrNode(height, ptrs.updated(thisIdx, newPtr)))
  }


  override def toStream : Stream[T] = {
    // Just get the children streams out in order:
    ptrs.foldLeft(Stream.empty[T]) { (oldStream, newPtr) =>
      if (newPtr != mem.nullPtr) {
        oldStream ++ (mem.deref(newPtr).asInstanceOf[Node[T,PtrT]].toStream)
      }
      else {
        oldStream
      }
    }
  }
  override def toString = {
    sciList("PtrNode(", height, ptrs, ")").mkString(", ")
  }
}

class Leaf[T,PtrT](val height : Short, val pos : Long, val value : T, mem : Allocator[PtrT])
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
  override def toStream = Stream(value)
  override def toString = {
    sciList("LeafNode(", height, pos, value, ")").mkString(", ")
  }
}
