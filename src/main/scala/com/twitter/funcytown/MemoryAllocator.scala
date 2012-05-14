package com.twitter.funcytown

class MemoryAllocator(implicit mf : Manifest[AnyRef]) extends Allocator[AnyRef] {
  override val nullPtr : AnyRef = null
  override def deref(ptr : AnyRef) = ptr
  override def empty[T](height : Short) : PtrNode[T,AnyRef] = {
    allocPtrNode(height, Block.alloc[AnyRef])
  }
  override def ptrOf[T](node : Node[T,AnyRef]) = node
  override def ptrOf[T](sn : List[T,AnyRef]) = sn

  protected val nilSeq = new List[AnyRef,AnyRef](null, null, this)
  override def nil[T] : List[T,AnyRef] = {
    nilSeq.asInstanceOf[List[T,AnyRef]]
  }
  def allocSeq[T](t : T, ptr : AnyRef) : List[T,AnyRef] = {
    new List(t, ptr, this)
  }
  override def allocLeaf[T](height : Short, pos : Long, value : T) = {
    new Leaf(height, pos, value, this)
  }
  override def allocPtrNode[T](height : Short, ptrs : Block[AnyRef]) = {
    new PtrNode[T,AnyRef](height, ptrs, this)
  }
}
