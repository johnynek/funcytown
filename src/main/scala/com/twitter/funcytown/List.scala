package com.twitter.funcytown

import scala.annotation.tailrec
import scala.collection.immutable.LinearSeq

object List {
  def apply[T](items : T*)(implicit mem : Allocator[_]) : List[T,_] = {
    from(items)(mem)
  }

  def from[T](iter : Iterable[T])(implicit mem : Allocator[_]) = {
    concat(iter,mem.nil[T])
  }

  def concat[T, U >: T, PtrT](iter : Iterable[U], seq : List[T,PtrT]) : List[U,PtrT] = {
    // This is safe because of the type constraint
    val sequ : List[U,PtrT] = seq
    // We reverse as we put in:
    iter.foldRight(sequ) { (newv, old) => newv :: old }
  }
}

/** Allocator agnostic immutable linked list
 */
class List[+T,PtrT](h : T, t : PtrT, val alloc : Allocator[PtrT]) extends LinearSeq[T] {
  // This is the cons operator:
  def debugStr : String = "(" + h + ", " + t + ")"
  def ::[U >: T](x: U) : List[U,PtrT] = alloc.allocSeq(x, alloc.ptrOf(this))

  def :::[U >: T](iter : Iterable[U]) : List[U,PtrT] = List.concat(iter, this)

  def ++[U >: T](iter : Iterable[U]) : List[U,PtrT] = {
    val seqiter : List[U,PtrT] = if (iter.isInstanceOf[List[U,_]] &&
      (iter.asInstanceOf[List[U,PtrT]].alloc == alloc)) {
      // No need to convert:
      iter.asInstanceOf[List[U,PtrT]]
    }
    else {
      List.from(iter)(alloc).asInstanceOf[List[U,PtrT]]
    }
    List.concat(this, seqiter)
  }

  override def foldRight[U](init : U)(foldfn : (T,U) => U) : U = {
    reverse.foldLeft(init) { (prev, item) => foldfn(item, prev) }
  }

  override def foldLeft[U](init : U)(foldfn : (U,T) => U) : U = {
    toStream.foldLeft(init)(foldfn)
  }

  def longLength : Long = {
    @tailrec
    def lenacc(acc : Long, list : Seq[_]) : Long = {
      if (list.isEmpty)
        acc
      else
        lenacc(acc + 1L, list.tail)
    }
    lenacc(0L, this)
  }

  override def length : Int = {
    val len = longLength
    if (len <= Int.MaxValue) len.toInt else error("Length: " + len + " can't fit in Int")
  }

  override def apply(idx : Int) : T = get(idx)
  @tailrec
  final def get(idx : Long) : T = {
    if (isEmpty) {
      error("List is empty, but get(" + idx + ") was called")
    }
    if (idx == 0) {
      h
    }
    else {
      tail.get(idx - 1L)
    }
  }
  override def isEmpty = (t == alloc.nullPtr)
  override def head = h
  override def iterator : Iterator[T] = toStream.iterator
  override lazy val reverse : List[T,PtrT] = {
    foldLeft(alloc.nil[T]) { (list, item) => item :: list }
  }

  override def tail = alloc.deref[List[T,PtrT]](t)
  override def toStream : Stream[T] = {
    if (isEmpty) {
      Stream.empty
    }
    else {
      Stream.cons(h, tail.toStream)
    }
  }
}
