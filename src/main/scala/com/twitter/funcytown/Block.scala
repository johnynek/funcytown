package com.twitter.funcytown

import scala.annotation.tailrec

object Block {
  @tailrec
  final def shiftOf(mask : Int, cnt : Int = 0) : Int = {
    if (mask == 0) cnt else shiftOf(mask >> 1, cnt + 1)
  }
  @tailrec
  final def bitmaskOf(cnt : Int, bm : Long = 0) : Long = {
    if (cnt <= 0) bm else bitmaskOf(cnt - 1, (bm << 1) | 1)
  }
  //val SHIFT = shiftOf(BITMASK)
  val SHIFT = 7 // 7 bits, 2^7 = 512, might be too broad a tree...
  val BITMASK = bitmaskOf(SHIFT).toInt
  def alloc[T](implicit mf : Manifest[T]) : Block[T] = {
    new Block[T](BITMASK, mf.newArray(BITMASK + 1))
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

sealed class Block[@specialized(Int,Long) N](bitmask : Int, ary : Array[N])
  (implicit mf : Manifest[N]) {

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
