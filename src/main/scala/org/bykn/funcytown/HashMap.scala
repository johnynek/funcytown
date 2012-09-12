package org.bykn.funcytown

import scala.collection.immutable.{List => sciList}
import scala.annotation.tailrec

import Block.{bitmaskOf, shiftOf}

object HashMap {
  val defaultBitmask = 31L // 32 initial size
  def apply[K,V](tups : Tuple2[K,V]*)(implicit mem : Allocator[_]) : HashMap[K,V,_] = {
    tups.foldLeft(empty[K,V](mem)) { (old, tupE) =>
      old + tupE
    }
  }
  def empty[K,V](implicit mem : Allocator[_]) : HashMap[K,V,_] = {
    // start with
    new HashMap(0, defaultBitmask, mem.empty(0))
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

  def rehash[V1 >: V, PtrU](newbitmask : Long, newMem : Allocator[PtrU]) : HashMap[K,V1,PtrU] = {
    if ( (newbitmask == bitmask) && (mem == newMem) ) {
      // if newMem == mem, PtrT == PtrU
      this.asInstanceOf[HashMap[K,V1,PtrU]]
    }
    else {
      val base = new HashMap[K,V1,PtrU](0L, newbitmask, newMem.empty(0))
      foldLeft(base) { (old, kv) => old.addRehash(kv, rehash=false) }
    }
  }

  // TODO we need to use an actual 64-bit hash, like Murmur
  protected def longHash(k : K) = k.hashCode.toLong

  protected def idxOf(hash : Long) = hash & bitmask

  protected def sameKey(tup1 : HashEntry[K,_], tup2 : HashEntry[K,_]) = tup1.sameKeyAs(tup2)

  override def +[V1 >: V](kv : (K,V1)) : HashMap[K,V1,PtrT] = {
    addRehash(kv, rehash=true)
  }

  // Add an item and optionally rehash
  def addRehash[V1 >: V](kv : (K,V1), rehash : Boolean) : HashMap[K,V1,PtrT] = {
    val hash = longHash(kv._1)
    val entryTup = new HashEntry(hash, kv._1, kv._2)
    var sizeDelta = 0L
    val oldValNewNode = node.map(idxOf(entryTup.hash)) { listPtr =>
      val list = mem.optionPtr(listPtr)
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
      mem.ptrOf(newList)
    }
    val newNode = oldValNewNode._2
    val newMap = new HashMap[K,V1,PtrT](longSize + sizeDelta, bitmask, newNode)
    if(rehash) {
      newMap.rehash(newMap.nextBitMask, node.allocator)
    }
    else {
      newMap
    }
  }

  protected def nextBitMask : Long = {
    // At least a factor of 4 overhead:
    bitmaskOf(shiftOf(longSize) + 2) max HashMap.defaultBitmask
  }

  override def -(key : K) : HashMap[K,V,PtrT] = {
    val hash = longHash(key)
    val entryTup = new HashEntry(hash, key, null)
    var sizeDelta = 0L
    val oldValNewNode = node.map(idxOf(entryTup.hash)) { listPtr =>
      val list = mem.optionPtr(listPtr)
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
      mem.ptrOf(newList)
    }
    val newNode = oldValNewNode._2
    val newMap = new HashMap[K,V,PtrT](longSize + sizeDelta, bitmask, newNode)
    newMap.rehash(newMap.nextBitMask, node.allocator)
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
    node.iterator
      .flatMap { listPtr =>
        val list = mem.deref[List[HashEntry[K,V], PtrT]](listPtr)
        list.iterator.map { _.keyValue }
      }
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
