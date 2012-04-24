package com.twitter.funcytown

import scala.annotation.tailrec

trait LowPriorityAllocator {
  implicit def default[T,_](implicit mf : Manifest[Node[T,_]]) : Allocator[T,_] = {
    new MemoryAllocator[T]
  }
}

class MemoryAllocator[T](implicit mf : Manifest[Node[T,_]]) extends Allocator[T, Node[T,_]] {
  override val nullPtr : Node[T,_] = null
  override def deref(ptr : Node[T,_]) = ptr.asInstanceOf[Node[T,Node[T,_]]]
  override def empty(height : Short) : PtrNode[T,Node[T,_]] = {
    allocPtrNode(0L, height, Block.alloc[Node[T,_]])
  }
  override def ptrOf(node : Node[T,Node[T,_]]) = node
  override def allocLeaf(height : Short, pos : Long, value : T) = {
    new Leaf(height, pos, value, this)
  }
  override def allocPtrNode(sz : Long, height : Short, ptrs : Block[Node[T,_]]) = {
    new PtrNode[T,Node[T,_]](sz, height, ptrs, this)
  }
}
