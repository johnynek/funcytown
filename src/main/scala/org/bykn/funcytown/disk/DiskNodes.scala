package org.bykn.funcytown.disk

import org.bykn.funcytown._

class ObjNode[T](val ptr : Long, val obj : T, override val mem : Allocator[Long]) extends ImmutableDagNode[Long] {
  override def selfPtr = ptr
  override lazy val pointers = pointersOf(obj)
  override def finalize { mem.free(ptr) }
}

class DiskPtrNode(val ptr : Long, height : Short, ptrs : Block[Long],
  override val mem : Allocator[Long])
  extends PtrNode[Long](height, ptrs, mem) with ImmutableDagNode[Long] {
  override def selfPtr = ptr
  override lazy val pointers = ptrs.foldLeft(Set[Long]()) { (set, ptr) =>
    if (ptr != mem.nullPtr) {
      set + ptr
    }
    else {
      set
    }
  }
  override def finalize { mem.free(ptr) }
}

class DiskSeq[T](val ptr : Long, h : Long, t : Long, override val mem : Allocator[Long])
  extends List[T,Long](h, t, mem) with ImmutableDagNode[Long] {
  override def selfPtr = ptr
  override lazy val pointers = Set(h,t).filter { _ != mem.nullPtr }
  override def finalize { mem.free(ptr) }
}
