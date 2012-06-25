package com.twitter.funcytown

import scala.collection.immutable.{List => sciList}

/**
 * This class is the underlying data structure behind the HashMap
 * (and also in principle an persistent vector class)
 * At each position it holds a pointer to some object.
 */
abstract class Node[PtrT] {
  def apply(pos : Long) = get(pos).get
  def get(pos : Long) : Option[PtrT] = findLeaf(pos).map { _.valuePtr }
  def isEmpty : Boolean
  def findLeaf(pos : Long) : Option[Leaf[PtrT]]
  def put(pos : Long, value : PtrT) : Node[PtrT] = {
    // Just replace:
    map(pos) { x => Some(value) }._2
  }
  def take(pos : Long) : (Option[PtrT], Node[PtrT]) = {
    // Just erase whatever is there:
    map(pos) { old => None }
  }
  def map(pos : Long)(fn : Option[PtrT] => Option[PtrT]) : (Option[PtrT], Node[PtrT])
  // These are all the leaf value pointers in this tree
  def toStream : Stream[PtrT]
}

class PtrNode[PtrT](val height : Short, val ptrs : Block[PtrT],
  mem : Allocator[PtrT])(implicit mf : Manifest[PtrT]) extends Node[PtrT] {
  override def isEmpty : Boolean = {
    ptrs.foldLeft(true) { (empty, ptr) =>
      empty && ((ptr == mem.nullPtr) || mem.deref[Node[PtrT]](ptr).isEmpty)
    }
  }
  override def findLeaf(pos : Long) : Option[Leaf[PtrT]] = {
    val (thisIdx, nextPos) = Block.toBlockIdx(height, pos)
    val nextPtr = ptrs(thisIdx)
    if (mem.nullPtr != nextPtr) {
      // nextPtr has height strictly greater
      val nextNode = mem.deref[Node[PtrT]](nextPtr)
      nextNode.findLeaf(nextPos)
    }
    else {
      None
    }
  }

  override def map(pos : Long)(fn : Option[PtrT] => Option[PtrT]) : (Option[PtrT], Node[PtrT]) = {
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
      val nextNode = mem.deref[Node[PtrT]](nextPtr)
      // Replace down the tree:
      val (old, resNode) = nextNode.map(nextPos)(fn)
      // If it is empty, we don't need to store, it, just put null
      (old, if (resNode.isEmpty) mem.nullPtr else mem.ptrOf(resNode))
    }
    (oldVal, mem.allocPtrNode(height, ptrs.updated(thisIdx, newPtr)))
  }


  override def toStream : Stream[PtrT] = {
    // Just get the children streams out in order:
    ptrs.foldLeft(Stream.empty[PtrT]) { (oldStream, newPtr) =>
      if (newPtr != mem.nullPtr) {
        oldStream ++ (mem.deref[Node[PtrT]](newPtr).toStream)
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

class Leaf[PtrT](val height : Short, val pos : Long, val valuePtr : PtrT, mem : Allocator[PtrT])
  (implicit mf : Manifest[PtrT])
  extends Node[PtrT] {
  override def isEmpty = false
  override def findLeaf(inpos : Long) = {
    if (pos == inpos) {
      Some(this)
    }
    else {
      None
    }
  }

  override def map(inpos : Long)(fn : Option[PtrT] => Option[PtrT]) : (Option[PtrT], Node[PtrT]) = {
    val oldValue = Some(valuePtr)
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
          .put(pos, valuePtr)
          .put(inpos, newValue.get)
        (None, resNode)
      }
      else {
        // None => None case, so do nothing:
        (None, this)
      }
    }
  }
  override def toStream = Stream(valuePtr)
  override def toString = {
    sciList("LeafNode(", height, pos, valuePtr, ")").mkString(", ")
  }
}
