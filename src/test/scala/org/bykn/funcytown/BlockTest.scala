package org.bykn.funcytown

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.choose

import scala.collection.immutable.{List => sciList}

import org.bykn.funcytown.Block.{shiftOf, bitmaskOf, fixHeight, toBlockIdx}

object BlockTest extends Properties("Block") {
  case class ShiftSize(s : Int)
  implicit val arbshift = Arbitrary { for(s <- choose(0, 64/Block.SHIFT)) yield ShiftSize(s) }

  case class BitCount(x: Int)
  implicit val arbcnt = Arbitrary { for( x <- choose(0, 63)) yield BitCount(x) }

  case class Position(p : Long)
  implicit val arbpos = Arbitrary { for( x <- choose(0, Long.MaxValue)) yield Position(x) }

  property("shiftOf and bitmask are inverses") = forAll { (bitVal : BitCount) =>
    shiftOf(bitmaskOf(bitVal.x)) == bitVal.x
  }

  property("fixHeight makes zero nextPos") = forAll { (height : ShiftSize, pos : Position) =>
    toBlockIdx(height.s, fixHeight(height.s, pos.p))._2 == 0L
  }

  property("fixHeight is identity if nextPos == 0") = forAll { (height : ShiftSize, pos : Position) =>
    val (nextIdx, nextPos) = toBlockIdx(height.s, pos.p)
    if (nextPos == 0L) {
      toBlockIdx(height.s, fixHeight(height.s, pos.p))._1 == nextIdx
    }
    else {
      (pos.p >> (Block.SHIFT * (height.s + 1))) > 0
    }
  }

  property("toBlockIdx is partially reversible") = forAll { (height : ShiftSize, pos : Position) =>
    val zeroed = (pos.p & (bitmaskOf(64) << (Block.SHIFT * height.s)))
    toBlockIdx(height.s, pos.p) == toBlockIdx(height.s, zeroed)
  }
}
