package com.twitter.funcytown

import scala.collection.immutable.{List => sciList}

object HashMap {
  def apply[K,V](tups : Tuple2[K,V]*)(implicit mem : Allocator[_]) : HashMap[K,V] = {
    tups.foldLeft(empty[K,V](mem)) { (old, tupE) =>
      old + tupE
    }
  }
  def empty[K,V](implicit mem : Allocator[_]) : HashMap[K,V] = {
    new HashMap(0, Block.BITMASK, mem.empty[sciList[HashEntry[K,V]]](0), mem)
  }
}

// This is so we make a special Kryo serializer for these objects
class HashEntry[K,+V](val hash : Long, val key : K, val value : V) {
  def sameKeyAs(that : HashEntry[K,_]) : Boolean = {
    // Do the hash comparison first, which should be cheaper
    (hash == that.hash) && (key == that.key)
  }

  lazy val keyValue : (K,V) = (key, value)
}

class HashMap[K,+V](val longSize : Long, bitmask : Long, node : Node[sciList[HashEntry[K,V]],_],
  val mem : Allocator[_]) extends Map[K,V] {

  def rehash[V1 >: V](newbitmask : Long, newMem : Allocator[_]) : HashMap[K,V1] = {
    if ( newbitmask == bitmask ) {
      this
    }
    else {
      val base = new HashMap[K,V1](0L, newbitmask, newMem.empty(0), newMem)
      foldLeft(base) { (old, kv) => old + kv }
    }
  }

  protected def longHash(k : K) = k.hashCode.toLong

  protected def idxOf(hash : Long) = hash & bitmask

  protected def sameKey(tup1 : HashEntry[K,_], tup2 : HashEntry[K,_]) = tup1.sameKeyAs(tup2)

  override def +[V1 >: V](kv : (K,V1)) : HashMap[K,V1] = {
    // Since the list is of any reference type, the cast below is safe:
    val castNode = node.asInstanceOf[Node[sciList[HashEntry[K,V1]],_]]
    val hash = longHash(kv._1)
    val entryTup = new HashEntry(hash, kv._1, kv._2)
    val oldValNewNode = castNode.map(idxOf(entryTup.hash)) { x =>
      Some(entryTup :: (x.getOrElse(Nil).filter { tup => !sameKey(entryTup, tup) }))
    }
    // Let's compute the new size:
    val newSize = if (oldValNewNode._1.flatMap { _.find { el => sameKey(entryTup, el) } }.isDefined) {
        // The key was present
        longSize
      }
      else {
        longSize + 1L
      }
    val newNode = oldValNewNode._2
    val nht = new HashMap[K,V1](newSize, bitmask, newNode, mem)
    if (longSize > 4 * bitmask) {
      // TODO maybe we should ask the allocator for the next allocator here
      nht.rehash((bitmask << 1) | 1L, mem)
    }
    else {
      nht
    }
  }

  override def -(key : K) : HashMap[K,V] = {
    val hashKey = longHash(key)
    val oldTupKey = new HashEntry(hashKey, key, null)
    val oldValNewNode = node.map(idxOf(hashKey)) { stored =>
      stored.flatMap { items =>
        val list = items.filter { el =>
          !sameKey(el, oldTupKey)
        }
        if (list.isEmpty) {
          None
        }
        else {
          Some(list)
        }
      }
    }
    // Let's compute the new size:
    val newSize = if (oldValNewNode._1.flatMap { _.find { el => sameKey(el, oldTupKey) } }.isDefined) {
        // The key was present
        longSize - 1
      }
      else {
        longSize
      }
    val newNode = oldValNewNode._2
    val nht = new HashMap[K,V](newSize, bitmask, newNode, mem)
    if (longSize < (bitmask / 4)) {
      nht.rehash((bitmask >> 1) | Block.BITMASK, mem)
    }
    else {
      nht
    }
  }

  override def get(key : K) : Option[V] = {
    val hashKey = longHash(key)
    val oldTupKey = new HashEntry(hashKey, key, null)
    node.get(idxOf(hashKey)).flatMap { list =>
      list.find { item => sameKey(item, oldTupKey) }
        .map { _.value }
    }
  }

  override def iterator : Iterator[(K,V)] = {
    node.toStream.flatMap { list =>
      list.map { _.keyValue }
    }.iterator
  }

  override def size : Int = {
    if (longSize <= Int.MaxValue) {
      longSize.toInt
    }
    else {
      error("Actual size: " + longSize + " too large to fit in Int")
    }
  }
}
