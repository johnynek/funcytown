package org.bykn.funcytown

class MemoryAllocator(implicit mf : Manifest[AnyRef]) extends Allocator[AnyRef] {
  override val nullPtr : AnyRef = null
  override def deref[T](ptr : AnyRef) = ptr.asInstanceOf[T]
  override def derefObj[T](ptr : AnyRef) = ptr.asInstanceOf[T]
  override def empty(height : Short) : PtrNode[AnyRef] = {
    allocPtrNode(height, Block.alloc[AnyRef])
  }
  override def ptrOf(node : Node[AnyRef]) = node
  override def ptrOf[T](sn : List[T,AnyRef]) = sn

  protected val nilSeq = new List[AnyRef,AnyRef](null, null, this)
  override def nil[T] : List[T,AnyRef] = {
    nilSeq.asInstanceOf[List[T,AnyRef]]
  }
  def allocObj[T](obj : T) : AnyRef = obj.asInstanceOf[AnyRef]
  def allocCons[T](head : AnyRef, tail : AnyRef) : List[T,AnyRef] = {
    new List(head, tail, this)
  }
  override def allocPtrNode(height : Short, ptrs : Block[AnyRef]) = {
    new PtrNode[AnyRef](height, ptrs, this)
  }
}
