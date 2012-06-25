package com.twitter.funcytown

import scala.collection.immutable.{List => sciList}
import scala.annotation.tailrec

object HashMap {
  def apply[K,V](tups : Tuple2[K,V]*)(implicit mem : Allocator[_]) : HashMap[K,V,_] = {
    tups.foldLeft(empty[K,V](mem)) { (old, tupE) =>
      old + tupE
    }
  }
  def empty[K,V](implicit mem : Allocator[_]) : HashMap[K,V,_] = {
    // Make the biggest bitmask we can fit in a 64 bit hash
    new HashMap(0, Block.bitmaskOf((64 / Block.SHIFT) * Block.SHIFT - 1), mem.empty(0))
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

class HashMap[K,+V, PtrT](val longSize : Long, bitmask : Long, node : Node[PtrT]) extends Map[K,V] {

  protected def mem = node.allocator

  def rehash[V1 >: V](newbitmask : Long, newMem : Allocator[PtrT]) : HashMap[K,V1,PtrT] = {
    if ( newbitmask == bitmask ) {
      this
    }
    else {
      val base = new HashMap[K,V1,PtrT](0L, newbitmask, newMem.empty(0))
      foldLeft(base) { (old, kv) => old + kv }
    }
  }

  protected def longHash(k : K) = k.hashCode.toLong

  protected def idxOf(hash : Long) = hash & bitmask

  protected def sameKey(tup1 : HashEntry[K,_], tup2 : HashEntry[K,_]) = tup1.sameKeyAs(tup2)

  override def +[V1 >: V](kv : (K,V1)) : HashMap[K,V1,PtrT] = {
    val hash = longHash(kv._1)
    val entryTup = new HashEntry(hash, kv._1, kv._2)
    var sizeDelta = 0L
    val oldValNewNode = node.map(idxOf(entryTup.hash)) { listPtrOpt =>
      val list = listPtrOpt
        .map { ptr => mem.deref[List[HashEntry[K,V1], PtrT]](ptr) }
        .getOrElse(mem.nil[HashEntry[K,V1]])
      val newList = if (list.isEmpty || list.forall( !_.sameKeyAs(entryTup) )) {
          //Just prepend:
          sizeDelta = 1L
          entryTup :: list
        }
        else {
          //Find the head-list and tail list:
          @tailrec
          def findKey(e : HashEntry[K,_], tail : List[HashEntry[K,V1],PtrT],
            head : List[HashEntry[K,V1],PtrT] = mem.nil[HashEntry[K,V1]]) :
            (List[HashEntry[K,V1],PtrT], List[HashEntry[K,V1],PtrT]) = {
            if( e.sameKeyAs(tail.head) ) {
              //We found the hit:
              (head, tail.tail)
            }
            else {
              val newHead = mem.allocCons(tail.h, mem.ptrOf(head))
              // Push onto the head:
              findKey(e, tail.tail, newHead)
            }
          }
          val (head, tail) = findKey(entryTup, list)
          entryTup :: (head ++ tail)
        }
      Some(mem.ptrOf(newList))
    }
    val newNode = oldValNewNode._2
    // TODO verify we don't need to rehash due to the sparsity of the Node data structure
    new HashMap[K,V1,PtrT](longSize + sizeDelta, bitmask, newNode)
  }

  override def -(key : K) : HashMap[K,V,PtrT] = {
    val hash = longHash(key)
    val entryTup = new HashEntry(hash, key, null)
    var sizeDelta = 0L
    val oldValNewNode = node.map(idxOf(entryTup.hash)) { listPtrOpt =>
      val list = listPtrOpt
        .map { ptr => mem.deref[List[HashEntry[K,V], PtrT]](ptr) }
        .getOrElse(mem.nil[HashEntry[K,V]])
      val newList = if (list.isEmpty || list.forall( !_.sameKeyAs(entryTup) )) {
          //Wasn't here to begin with:
          list
        }
        else {
          //Find the head-list and tail list:
          @tailrec
          def findKey(e : HashEntry[K,_], tail : List[HashEntry[K,V],PtrT],
            head : List[HashEntry[K,V],PtrT] = mem.nil[HashEntry[K,V]]) :
            (List[HashEntry[K,V],PtrT], List[HashEntry[K,V],PtrT]) = {
            if( e.sameKeyAs(tail.head) ) {
              //We found the hit:
              (head, tail.tail)
            }
            else {
              val newHead = mem.allocCons(tail.h, mem.ptrOf(head))
              // Push onto the head:
              findKey(e, tail.tail, newHead)
            }
          }
          val (head, tail) = findKey(entryTup, list)
          // Remove it:
          sizeDelta = -1L
          head ++ tail
        }
      Some(mem.ptrOf(newList))
    }
    val newNode = oldValNewNode._2
    // TODO verify we don't need to rehash due to the sparsity of the Node data structure
    new HashMap[K,V,PtrT](longSize + sizeDelta, bitmask, newNode)
  }

  override def get(key : K) : Option[V] = {
    val hashKey = longHash(key)
    val oldTupKey = new HashEntry(hashKey, key, null)
    node.get(idxOf(hashKey)).flatMap { listPtr =>
      val list = mem.deref[List[HashEntry[K,V], PtrT]](listPtr)
      list
        .find { item => sameKey(item, oldTupKey) }
        .map { _.value }
    }
  }

  override def iterator : Iterator[(K,V)] = {
    node.toStream
      .flatMap { listPtr =>
        val list = mem.deref[List[HashEntry[K,V], PtrT]](listPtr)
        list.map { _.keyValue }
      }
      .iterator
  }

  override lazy val size : Int = {
    if (longSize <= Int.MaxValue) {
      longSize.toInt
    }
    else {
      error("Actual size: " + longSize + " too large to fit in Int")
    }
  }
}
