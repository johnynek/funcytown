package com.twitter.funcytown

import scala.annotation.tailrec

object HashNode {
  def apply[K,V](tups : Tuple2[K,V]*)(implicit mem : Allocator[List[(Long,K,V)],_]) : HashNode[K,V] = {
    tups.foldLeft(empty[K,V](mem)) { (old, tupE) =>
      old + tupE
    }
  }
  def empty[K,V](implicit mem : Allocator[List[(Long,K,V)],_]) : HashNode[K,V] = {
    new HashNode(Block.BITMASK, mem.empty(0), mem)
  }
}

class HashNode[K,+V](bitmask : Long, node : Node[List[(Long,K,V)],_],
  mem : Allocator[List[(Long,K,V)],_]) extends Map[K,V] {

  def rehash[V1 >: V](newbitmask : Long, newMem : Allocator[List[(Long,K,V1)],_]) : HashNode[K,V1] = {
    if ( newbitmask == bitmask ) {
      this
    }
    else {
      val base = new HashNode[K,V1](newbitmask, newMem.empty(0), newMem)
      foldLeft(base) { (old, kv) => old + kv }
    }
  }

  protected def longHash(k : K) = (k.hashCode.toLong & bitmask)

  override def +[V1 >: V](kv : (K,V1)) : HashNode[K,V1] = {
    // Since the list is of any reference type, the cast below is safe:
    val castNode = node.asInstanceOf[Node[List[(Long,K,V1)],_]]
    val castMem = mem.asInstanceOf[Allocator[List[(Long,K,V1)],_]]
    val entryTup = (longHash(kv._1), kv._1, kv._2)
    val newNode = castNode.map(entryTup._1) { x =>
      Some(entryTup :: (x.getOrElse(Nil).filter { _._2 != kv._1 }))
    }._2
    val nht = new HashNode[K,V1](bitmask, newNode, castMem)
    if (newNode.size * 2 > bitmask) {
      // TODO maybe we should ask the allocator for the next allocator here
      nht.rehash((bitmask << 1) | 1L, castMem)
    }
    else {
      nht
    }
  }

  override def -(key : K) : HashNode[K,V] = {
    val hashKey = longHash(key)
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
    val nht = new HashNode[K,V](bitmask, newNode, mem)
    if (newNode.size / 4 < bitmask) {
      nht.rehash((bitmask >> 1) | Block.BITMASK, mem)
    }
    else {
      nht
    }
  }

  override def get(key : K) : Option[V] = {
    val hashKey = longHash(key)
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
