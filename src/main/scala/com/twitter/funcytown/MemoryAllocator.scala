package com.twitter.funcytown

import scala.annotation.tailrec

trait LowPriorityAllocator {
  implicit def default(implicit mf : Manifest[AnyRef]) : Allocator[_] = {
    new MemoryAllocator
  }
}

class MemoryAllocator(implicit mf : Manifest[AnyRef]) extends Allocator[AnyRef] {
  override val nullPtr : AnyRef = null
  override def deref(ptr : AnyRef) = ptr
  override def empty[T](height : Short) : PtrNode[T,AnyRef] = {
    allocPtrNode(0L, height, Block.alloc[AnyRef])
  }
  override def ptrOf[T](node : Node[T,AnyRef]) = node
  override def ptrOf[T](sn : SeqNode[T,AnyRef]) = sn

  protected val nilSeq = new SeqNode[AnyRef,AnyRef](null, null, this)
  override def nil[T] : SeqNode[T,AnyRef] = {
    nilSeq.asInstanceOf[SeqNode[T,AnyRef]]
  }
  def allocSeq[T](t : T, ptr : AnyRef) : SeqNode[T,AnyRef] = {
    new SeqNode(t, ptr, this)
  }
  override def allocLeaf[T](height : Short, pos : Long, value : T) = {
    new Leaf(height, pos, value, this)
  }
  override def allocPtrNode[T](sz : Long, height : Short, ptrs : Block[AnyRef]) = {
    new PtrNode[T,AnyRef](sz, height, ptrs, this)
  }
}
