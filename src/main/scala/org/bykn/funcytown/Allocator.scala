package org.bykn.funcytown

trait Allocator[@specialized(Long) PtrT] {
  val nullPtr : PtrT
  def optionPtr(ptr : PtrT) = if(ptr != nullPtr) Some(ptr) else None
  def derefOpt[T](ptr : PtrT) = optionPtr(ptr).map { notnullptr => deref[T](notnullptr) }

  def deref[T](ptr : PtrT) : T
  // This is the inverse of allocObj
  def derefObj[T](ptr : PtrT) : T
  def ptrOf(node : Node[PtrT]) : PtrT
  def ptrOf[T](sn : List[T,PtrT]) : PtrT
  def empty(height : Short): PtrNode[PtrT]
  def nil[T] : List[T,PtrT]
  // If we want to cache or do some other post processing
  def afterAlloc[Col](ptr : PtrT, obj : Col) : Col = obj
  // Store the object into the memory and return a pointer to it
  def allocObj[T](obj : T) : PtrT
  def allocCons[T](head : PtrT, tail : PtrT) : List[T,PtrT]
  def allocPtrNode(height : Short, ptrs : Block[PtrT]) : PtrNode[PtrT]
  // When will no longer use this node, free may be called and the allocator
  // may release the memory
  def free(ptr : PtrT) { }
}

/*
 * Anything implementing this trait is both immutable and part of an
 * acyclic graph, so when finding the reachable set, we don't have to worry
 * about that set changing in time
 */
trait ImmutableDagNode[PtrT] {
  def selfPtr : PtrT
  def mem : Allocator[PtrT]
  def pointers : Set[PtrT]

  // Allows a node with data to recurse on that data
  // Be careful: if you mix nodes of different ptr types,
  // due to type erasure of the PtrT, this method is not safe.
  def pointersOf[T](t : T) : Set[PtrT] = {
    if(t.isInstanceOf[ImmutableDagNode[PtrT]]) {
      //We recurse:
      t.asInstanceOf[ImmutableDagNode[PtrT]].pointers
    }
    else {
      Set[PtrT]()
    }
  }
}
