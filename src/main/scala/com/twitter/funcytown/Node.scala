package com.twitter.funcytown

import scala.annotation.tailrec

object Block {
  val SHIFT = 7
  val BITMASK = 0x7f // 0111 1111
  def alloc[T] : Block[T] = {
    new Block[T](BITMASK,
      (new Array[AnyRef](BITMASK + 1)).asInstanceOf[Array[T]]
    )
  }

  def toBlockIdx(height : Int, pos : Long) : (Int, Long) = {
    @tailrec
    def bidx(acc : Long, iheight : Int, ipos : Long) : (Int,Long) = {
      if(iheight == 0) {
        ((ipos & BITMASK).toInt, acc)
      }
      else {
        //decrement the height by one
        bidx((acc << SHIFT) + (ipos & BITMASK), iheight - 1, ipos >> SHIFT)
      }
    }
    bidx(0L, height, pos)
  }
}

sealed class Block[N](bitmask : Int, ary : Array[N]) {
  private def toIdx(in : Long) = (in & bitmask).toInt
  def apply(idx : Long) : N = ary(toIdx(idx))
  def updated(idx : Long, value : N) : Block[N] = {
    if (ary(toIdx(idx)) == value) {
      // No need to update:
      this
    }
    else {
      val newAry = ary.clone
      newAry(toIdx(idx)) = value
      new Block[N](bitmask, newAry)
    }
  }

  def foldLeft[M](init : M)(foldfn : (M,N) => M) : M = ary.foldLeft(init)(foldfn)
}

object Node {
  protected val emptyBlk = Block.alloc[Node[AnyRef]]
  private val emptyPtr = PtrNode[AnyRef](0, 0, emptyBlk)
  def empty[T] : Node[T] = emptyPtr.asInstanceOf[Node[T]]
  def empty[T](height : Int) : PtrNode[T] = {
    PtrNode[T](0L, height, emptyPtr.ptrs.asInstanceOf[Block[Node[T]]])
  }
}

sealed abstract class Node[T] {
  def apply(pos : Long) = get(pos).get
  def get(pos : Long) : Option[T] = findLeaf(pos).map { _.value }
  def isEmpty : Boolean = (size == 0)
  def findLeaf(pos : Long) : Option[Leaf[T]]
  def put(pos : Long, value : T) : Node[T] = {
    // Just replace:
    map(pos) { x => Some(value) }._2
  }
  def take(pos : Long) : (Option[T], Node[T]) = {
    // Just erase whatever is there:
    map(pos) { old => None }
  }
  def size : Long
  def map(pos : Long)(fn : Option[T] => Option[T]) : (Option[T], Node[T])
  def toStream : Stream[T]
}

case class PtrNode[T](sz : Long, val height : Int, val ptrs : Block[Node[T]]) extends Node[T] {
  final def findLeaf(pos : Long) : Option[Leaf[T]] = {
    val (thisIdx, nextPos) = Block.toBlockIdx(height, pos)
    val nextPtr = ptrs(thisIdx);
    if (null != nextPtr) {
      nextPtr.findLeaf(nextPos)
    }
    else {
      None
    }
  }

  def map(pos : Long)(fn : Option[T] => Option[T]) : (Option[T], Node[T]) = {
    if (pos < maxPos) {
      // This position cannot possibly be in the tree
      val (thisIdx, nextPos) = Block.toBlockIdx(height, pos)
      val nextNode = ptrs(thisIdx)
      val (oldVal, newNode, szdelta) = if (null == nextNode) {
        val value = fn(None)
        if (value.isDefined) {
          // This is a new value:
          (None, Leaf(height, nextPos, value.get),1L)
        }
        else {
          // Mapping None => None, weird, but okay.
          (None, null, 0L)
        }
      }
      else {
        // Replace down the tree:
        val (old, resNode) = nextNode.map(nextPos)(fn)
        (old, resNode, resNode.size - nextNode.size)
      }
      (oldVal, PtrNode[T](sz + szdelta, height, ptrs.updated(thisIdx, newNode)))
    }
    else {
      val newValue = fn(None)
      if (newValue.isDefined) {
        //We need a level above us:
        val newBlock = Block.alloc[Node[T]].updated(0, this)
        PtrNode[T](sz, height + 1, newBlock).map(pos)(fn)
      }
      else {
        //None => None
        (None, this)
      }
    }
  }

  private def maxPos : Long = {
    // This is 2^(7*(height + 1))
    1 << (Block.SHIFT * (height + 1))
  }

  def size = sz

  def toStream : Stream[T] = {
    // Just get the children streams out in order:
    ptrs.foldLeft(Stream.empty[T]) { (oldStream, newNode) =>
      if (newNode != null) {
        oldStream ++ newNode.toStream
      }
      else {
        oldStream
      }
    }
  }
}

case class Leaf[T](val height : Int, val pos : Long, val value : T) extends Node[T] {
  override def isEmpty = false
  def findLeaf(inpos : Long) : Option[Leaf[T]] = {
    if (pos == inpos) {
      Some(this)
    }
    else {
      None
    }
  }

  def map(inpos : Long)(fn : Option[T] => Option[T]) : (Option[T], Node[T]) = {
    val oldValue = Some(value)
    if (pos == inpos) {
      val newValue = fn(oldValue)
      if (newValue.isDefined) {
        // Replace:
        (oldValue, Leaf[T](height, pos, newValue.get))
      }
      else {
        // This node is now deleted:
        (oldValue, Node.empty[T](height))
      }
    }
    else {
      //We have to deepen the tree here:
      val newValue = fn(None)
      if (newValue.isDefined) {
        assert(height > 0)
        val resNode = Node.empty[T](height)
          .put(pos, value)
          .put(inpos, newValue.get)
        (None, resNode)
      }
      else {
        // None => None case, so do nothing:
        (None, this)
      }
    }
  }
  def size = 1L
  def toStream = Stream(value)
}

object HashNode {
  def apply[K,V](tups : Tuple2[K,V]*) : HashNode[K,V] = {
    tups.foldLeft(empty[K,V]) { (old, tupE) =>
      old + tupE
    }
  }
  def empty[K,V] : HashNode[K,V] = new HashNode(Node.empty[List[(Long,K,V)]])
}

class HashNode[K,+V](node : Node[List[(Long,K,V)]]) extends Map[K,V] {
  override def +[V1 >: V](kv : (K,V1)) : HashNode[K,V1] = {
    val castNode = node.asInstanceOf[Node[List[(Long,K,V1)]]]
    val entryTup = (kv._1.hashCode.toLong, kv._1, kv._2)
    val newNode = castNode.map(entryTup._1) { x =>
      Some(entryTup :: x.getOrElse(Nil))
    }._2
    new HashNode[K,V1](newNode)
  }

  override def -(key : K) : HashNode[K,V] = {
    val hashKey = key.hashCode.toLong
    val newNode = node.map(hashKey) { stored =>
      stored.flatMap { items =>
        val list = items.filter { el =>
          (el._2 != key)
        }
        if (list.isEmpty) {
          None
        }
        else {
          Some(list)
        }
      }
    }._2
    new HashNode[K,V](newNode)
  }

  override def get(key : K) : Option[V] = {
    val hashKey = key.hashCode.toLong
    node.get(hashKey).flatMap { list =>
      list.find { item => item._2 == key }
        .map { _._3 }
    }
  }

  override def iterator : Iterator[(K,V)] = {
    node.toStream.flatMap { list =>
      list.map { tup => (tup._2, tup._3) }
    }.iterator
  }
}
