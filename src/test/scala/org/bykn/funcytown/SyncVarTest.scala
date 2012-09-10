package org.bykn.funcytown

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll

import scala.collection.immutable.{List => sciList}

object SyncVarTest extends Properties("SyncVar") {

  property("Get should return") = forAll { (x : Int) =>
    val sx = new SyncVar(x)
    sx.get == x
  }
  property("Swap should work") = forAll { (x : Int, y : Int) =>
    val sx = new SyncVar(x)
    (sx.get == x) && (sx.swap(y) == x) && (sx.get == y)
  }
  property("parallel mutateIdem should work") = forAll { (l : sciList[Int]) =>
    val sync = new SyncVar(0)
    l.par.map { v => sync.mutateIdem { _ + v } }
    sync.get == l.sum
  }
  property("parallel mutate should work") = forAll { (l : sciList[Int]) =>
    val sync = new SyncVar(0)
    l.par.map { v => sync.mutate { _ + v } }
    sync.get == l.sum
  }
  property("parallel effectIdem should work") = forAll { (l : sciList[Int]) =>
    val sync = new SyncVar(0)
    val mapped = l.par.map { v => sync.effectIdem { o => (o + v, v*v) } }
    sync.get == l.sum && mapped.sum == l.map { x => x*x }.sum
  }
  property("parallel effect should work") = forAll { (l : sciList[Int]) =>
    val sync = new SyncVar(0)
    val mapped = l.par.map { v => sync.effect { o => (o + v, v*v) } }
    sync.get == l.sum && mapped.sum == l.map { x => x*x }.sum
  }
}
