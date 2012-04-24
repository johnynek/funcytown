package com.twitter.funcytown

import scala.annotation.tailrec

class DiskAllocator[T](implicit mf : Manifest[T]) extends Allocator[T, Long] {
  // TODO actually serialize to disk
  val rng = new java.util.Random
  var ht = Map[Long,AnyRef]()

  override val nullPtr : Long = 0L
  override def deref(ptr : Long) = {
    ht(ptr).asInstanceOf[Node[T,Long]]
  }
  override def empty(height : Short) : PtrNode[T,Long] = {
    allocPtrNode(0L, height, Block.alloc[Long])
  }
  override def ptrOf(node : Node[T,Long]) = {
    node match {
      case leaf : DiskLeaf[_] => leaf.ptr
      case ptrNode : DiskPtrNode[_] => ptrNode.ptr
      case _ => error("Invalid node")
    }
  }
  override def allocLeaf(height : Short, pos : Long, value : T) = {
    val ptr = rng.nextInt.toLong
    val leaf = new DiskLeaf[T](ptr, height, pos, value, this)
    ht = ht + (ptr -> leaf)
    leaf
  }
  override def allocPtrNode(sz : Long, height : Short, ptrs : Block[Long]) = {
    val ptr = rng.nextInt.toLong
    val ptrNode = new DiskPtrNode[T](ptr, sz, height, ptrs, this)
    ht = ht + (ptr -> ptrNode)
    ptrNode
  }
}

class DiskLeaf[T](val ptr : Long, hs : Short, ps : Long, v : T, m : Allocator[T,Long]) extends
  Leaf[T,Long](hs, ps, v, m)

class DiskPtrNode[T](val ptr : Long, sz : Long, height : Short, ptrs : Block[Long],
  mem : Allocator[T,Long]) extends PtrNode[T,Long](sz, height, ptrs, mem)
