package org.bykn.funcytown.gc

import org.bykn.funcytown.{Allocator, ImmutableDagNode}

private[funcytown] abstract class MemorySearch(alloc : Allocator[Long]) extends
  GraphSearch[Long, UnallocatedSpace] {
  def visit(node : Long, visited : UnallocatedSpace) = visited - regionOf(node)
  def haveVisited(node : Long, visited : UnallocatedSpace) = {
    // We remove the region when we visit
    visited.findRegionContaining(node).isEmpty
  }
  def regionOf(ptr : Long) : ContiguousRegion
  def neighborsOf(ptr : Long) = {
    alloc.deref[ImmutableDagNode[Long]](ptr).pointers
  }
}

