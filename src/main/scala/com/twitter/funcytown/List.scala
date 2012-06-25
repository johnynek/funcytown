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
    sequ.asList(iter).map { List.concatList(_, sequ) }
      .getOrElse {
        // We reverse as we put in:
        iter.foldRight(sequ) { (newv, old) => newv :: old }
      }
  }
  // If you know both are lists, this is the best one to call
  def concatList[T, PtrT](left : List[T,PtrT], right : List[T,PtrT]) : List[T,PtrT] = {
    //Use the alloc for the right:
    val alloc = right.alloc
    left.foldRightPtr(right) { (newPtr, oldList) =>
      alloc.allocCons[T](newPtr, alloc.ptrOf(oldList))
    }
  }
}

/** Allocator agnostic immutable linked list
 */
class List[+T,PtrT](val h : PtrT, val t : PtrT, val alloc : Allocator[PtrT]) extends LinearSeq[T] {
  // This is the cons operator:
  def debugStr : String = "(" + h + ", " + t + ")"
  def ::[U >: T](x: U) : List[U,PtrT] = {
    alloc.allocCons[U](alloc.allocObj(x), alloc.ptrOf(this))
  }

  def :::[U >: T](iter : Iterable[U]) : List[U,PtrT] = {
    List.concat(iter, this)
  }

  def ++[U >: T](iter : Iterable[U]) : List[U,PtrT] = {
    val iterL = asList(iter)
      .getOrElse { List.from(iter)(alloc).asInstanceOf[List[U,PtrT]] }
    List.concatList(this, iterL)
  }

  def asList[U](iter : Iterable[U]) : Option[List[U,PtrT]] = {
    if (iter.isInstanceOf[List[U,_]] &&
      (iter.asInstanceOf[List[U,PtrT]].alloc == alloc)) {
      Some(iter.asInstanceOf[List[U,PtrT]])
    }
    else {
      None
    }
  }

  override def filter(p : (T) => Boolean) : List[T,PtrT] = {
    @tailrec
    def getLastTrue(trueList : List[T,PtrT], check : List[T,PtrT]) : List[T,PtrT] = {
      if(check.isEmpty) {
        trueList
      }
      else if(p(check.head)) {
        //we are still true:
        getLastTrue(trueList, check.tail)
      }
      else {
        //Reset at our tail:
        getLastTrue(check.tail, check.tail)
      }
    }
    val trueSuffix = getLastTrue(this, this)
    // push into the stack until we reach the suffix:
    @tailrec
    def buildStack(stack : List[T,PtrT], thisPos : List[T,PtrT]) : List[T,PtrT] = {
      if(thisPos.t == trueSuffix.t) {
        // then we have filled the stack
        stack
      }
      else if(p(thisPos.head)) {
        // Push into the stack:
        val newStack = alloc.allocCons(thisPos.h, alloc.ptrOf(stack))
        buildStack(newStack, thisPos.tail)
      }
      else {
        //Just skip this one:
        buildStack(stack, thisPos.tail)
      }
    }
    val filteredStack = buildStack(alloc.nil[T], this)
    //Just pop off the stack:
    filteredStack.foldLeftPtr(trueSuffix) { (oldList, ptr) =>
      alloc.allocCons(ptr, alloc.ptrOf(oldList))
    }
  }
  override def filterNot( p : (T) => Boolean) = filter { !p(_) }

  override def foldRight[U](init : U)(foldfn : (T,U) => U) : U = {
    reverse.foldLeft(init) { (prev, item) => foldfn(item, prev) }
  }
  def foldRightPtr[U](init : U)(foldfn : (PtrT,U) => U) : U = {
    reverse.foldLeftPtr(init) { (prev, item) => foldfn(item, prev) }
  }

  override def foldLeft[U](init : U)(foldfn : (U,T) => U) : U = {
    toStream.foldLeft(init)(foldfn)
  }
  def foldLeftPtr[U](init : U)(foldfn : (U, PtrT) => U) : U = {
    toPtrStream.foldLeft(init)(foldfn)
  }

  override lazy val last = {
    assert(!isEmpty, "last called on an empty funcytown.List")
    @tailrec
    def lastOf(list : List[T,PtrT]) : T = {
      if(list.tail.isEmpty) { list.head }
      else { lastOf(list.tail) }
    }
    lastOf(this)
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

  override def size = length

  override def apply(idx : Int) : T = get(idx)
  @tailrec
  final def get(idx : Long) : T = {
    if (isEmpty) {
      error("List is empty, but get(" + idx + ") was called")
    }
    if (idx == 0) {
      head
    }
    else {
      tail.get(idx - 1L)
    }
  }
  override def isEmpty = (t == alloc.nullPtr)
  override lazy val head = alloc.derefObj[T](h)
  override def iterator : Iterator[T] = toStream.iterator
  override lazy val reverse : List[T,PtrT] = {
    foldLeftPtr(alloc.nil[T]) { (tail, ptr) =>
      alloc.allocCons[T](ptr, alloc.ptrOf(tail))
    }
  }

  override lazy val tail = alloc.deref[List[T,PtrT]](t)
  def toPtrStream : Stream[PtrT] = {
    if (isEmpty) {
      Stream.empty
    }
    else {
      Stream.cons(h, tail.toPtrStream)
    }
  }
  override def toStream : Stream[T] = {
    if (isEmpty) {
      Stream.empty
    }
    else {
      Stream.cons(head, tail.toStream)
    }
  }
}
