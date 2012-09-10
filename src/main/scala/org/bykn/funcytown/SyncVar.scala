package org.bykn.funcytown

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec

class SyncVar[T](init : T) {
  private val value = new AtomicReference[Option[T]](Some(init))

  def get : T = getValue.get
  // Set the value and get the starting value
  def swap(newv : T) : T = effectIdem { old => (newv, old) }

  // fnidem MUST BE IDEMPOTENT. It can't have side-effects
  @tailrec
  final def mutateIdem[U <: T](fnidem : (T) => U) : U = {
    val v = getValue
    val newU = fnidem(v.get)
    if (value.compareAndSet(v, Some(newU))) {
      //Success!
      newU
    }
    else {
      //Failed! retry:
      mutateIdem(fnidem)
    }
  }
  // Prefer mutateIdem, which is faster
  // fn is called exactly once inside a private lock. the result is returned
  def mutate[U <: T](fn : (T) => U) : U = {
    var myVal = getLock
    val myU = try {
      val newU = fn(myVal.get)
      myVal = Some(newU)
      newU
    }
    finally {
      // If we throw in fn, this resets to init val
      // if we don't throw, it sets to the new val
      setAndUnlock(myVal)
    }
    myU
  }
  // read with a side effect
  // Prefer effectIdem
  def effect[U <:T, S]( fn : (T) => (U,S) ) : S = {
    var myVal = getLock
    val resultS = try {
      val (myU, myS) = fn(myVal.get)
      //Update the var:
      myVal = Some(myU)
      myS
    }
    finally {
      // If we throw in fn, this resets to init val
      // if we don't throw, it sets to the new val
      setAndUnlock(myVal)
    }
    resultS
  }

  // fn MUST BE IDEMPOTENT. It can't have side-effects
  // read with a side effect
  @tailrec
  final def effectIdem[U <:T, S]( fn : (T) => (U,S) ) : S = {
    val v = getValue
    val (newV, newS) = fn(v.get)
    if(value.compareAndSet(v, Some(newV))) {
      //Success!
      newS
    }
    else {
      //Retry:
      effectIdem(fn)
    }
  }

////////////////////////////
// Private

  @tailrec
  private def getLock : Some[T] = {
    val v = getValue
    if(value.compareAndSet(v, None)) {
      //We got the lock:
      v
    }
    else {
      // It changed, we have to retry:
      getLock
    }
  }

  @tailrec
  private def getValue : Some[T] = {
    value.get match {
      case v : Some[T] => v
      case None => {
        Thread.sleep(0L, 100) //100 nanosecs
        getValue
      }
    }
  }

  private def setAndUnlock(v : Some[T]) : Unit = {
    if (value.getAndSet(v) != None) {
      //This should never happen:
      throw new Exception("Expected None")
    }
  }

}
