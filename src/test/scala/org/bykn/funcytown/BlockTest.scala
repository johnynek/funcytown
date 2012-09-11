package org.bykn.funcytown

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.choose

import scala.collection.immutable.{List => sciList}

import org.bykn.funcytown.Block.{shiftOf, bitmaskOf}

object BlockShiftTest extends Properties("BlockShift") {
  implicit val arbint = Arbitrary { choose(0, 63) }
  property("shiftOf and bitmask are inverses") = forAll { (x : Int) =>
    shiftOf(bitmaskOf(x)) == x
  }
}
