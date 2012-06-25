package com.twitter.funcytown

class SyncVar[@specialized(Int,Long) T](init : T) {
  private val lock = new Object
  private var value = init
  // fn is called exactly once inside a private lock. the result is returned
  def mutate[U <: T](fn : (T) => U) : U = {
    lock.synchronized {
      val valueU = fn(value)
      value = valueU
      valueU
    }
  }
  // read with a side effect
  def effect[U <:T, S]( fn : (T) => (U,S) ) : S = {
    lock.synchronized {
      val valueSide = fn(value)
      value = valueSide._1
      valueSide._2
    }
  }
}
