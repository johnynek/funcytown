package com.twitter.funcytown

trait Allocator[@specialized(Long) PtrT] {
  val nullPtr : PtrT
  def deref(ptr : PtrT) : AnyRef
  def ptrOf[T](node : Node[T,PtrT]) : PtrT
  def ptrOf[T](sn : List[T,PtrT]) : PtrT
  def empty[T](height : Short): PtrNode[T,PtrT]
  def nil[T] : List[T,PtrT]
  // If we want to cache or do some other post processing
  def afterAlloc[Col](ptr : PtrT, obj : Col) : Col = obj
  def allocSeq[T](t : T, ptr : PtrT) : List[T,PtrT]
  def allocLeaf[T](height : Short, pos : Long, value : T) : Leaf[T,PtrT]
  def allocPtrNode[T](height : Short, ptrs : Block[PtrT]) : PtrNode[T,PtrT]
  // When will no longer use this node, free may be called and the allocator
  // may release the memory
  def free(ptr : PtrT) { }
}
