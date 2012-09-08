package org.bykn.funcytown

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
  val SHIFT = 5
  val BITMASK = bitmaskOf(SHIFT).toInt
  def alloc[T](implicit mf : Manifest[T]) : Block[T] = {
    new Block[T](BITMASK, mf.newArray(BITMASK + 1))
  }

  def toBlockIdx(height : Int, pos : Long) : (Int, Long) = {
    val blocks = 64 / SHIFT
    val nextBlocks = (blocks - height - 1)
    val thisPos = ((pos >> (SHIFT * nextBlocks)) & BITMASK).toInt
    val nextPos = pos & bitmaskOf(SHIFT * nextBlocks)
    (thisPos, nextPos)
  }

  def fromSparse[N](sparse : Map[Int,N], sval : N)(implicit mf : Manifest[N]) : Block[N] = {
    val array = mf.newArray(BITMASK + 1)
    //just go through and update:
    (0 to BITMASK).foreach { idx =>
      array(idx) = sparse.getOrElse(idx, sval)
    }
    new Block[N](BITMASK, array)
  }
}

sealed class Block[@specialized(Int,Long) N](bitmask : Int, ary : Array[N]) {

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
  def toSparse(sparseVal : N) : Map[Int, N] = {
    ary.zipWithIndex.foldLeft(Map[Int,N]()) { (oldMap, valIdx) =>
      if( valIdx._1 != sparseVal ) {
        oldMap + (valIdx._2 -> valIdx._1)
      }
      else {
        oldMap
      }
    }
  }
  def foldLeft[M](init : M)(foldfn : (M,N) => M) : M = ary.foldLeft(init)(foldfn)
  override def toString = ary.toString
}
