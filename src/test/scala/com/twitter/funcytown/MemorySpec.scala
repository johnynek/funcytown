package com.twitter.funcytown

import org.specs._

import scala.collection.immutable.{List => sciList}

class Into[U](uin : U) {
  def -->[T](fn : (U) => T) : T = fn(uin)
}

class SetSearch(graph : Map[Int,Iterable[Int]]) extends GraphSearch[Int,Set[Int]] {
  val empty = Set[Int]()
  def visit(node : Int, visited : Set[Int]) = (visited + node)
  def haveVisited(node : Int, visited : Set[Int]) = visited(node)
  def neighborsOf(from : Int) = graph.getOrElse(from, sciList[Int]())
}

class MemorySpec extends Specification {
  noDetailedDiffs()
  // Enable the --> syntax
  implicit def toInto[U](ua : U) = new Into(ua)

  "UnallocatedSpace" should {
    "contain lower bounds, but not strict upper" in {
      val space = UnallocatedSpace.range(100, 1000)
      space.findRegionContaining(99).isEmpty must be_==(true)
      space.findRegionContaining(100).isDefined must be_==(true)
      space.findRegionContaining(900).isDefined must be_==(true)
      space.findRegionContaining(999).isDefined must be_==(true)
      space.findRegionContaining(1000).isEmpty must be_==(true)
    }
    "not contain space removed" in {
      val space = UnallocatedSpace.range(100, 1000)
      def removeAndCheck(sp : UnallocatedSpace, reg : ContiguousRegion) : UnallocatedSpace = {
        val newsp = sp - reg
        newsp.contains(reg) must be_==(false)
        newsp
      }
      space --> { sp =>
          sp.contains(ContiguousRegion(200,100)) must be_==(true)
          sp.size must be_==(900L)
          removeAndCheck(sp,  ContiguousRegion(200, 100))
        } --> { sp =>
          sp.size must be_==(800L)
          sp.contains(ContiguousRegion(200,100)) must be_==(false)
          val sp2 = removeAndCheck(sp,  ContiguousRegion(200, 100))
          (sp2 == sp) must be_==(true)
          sp2
        } --> { sp =>
          val sp2 = removeAndCheck(sp,  ContiguousRegion(300, 100))
          sp2.size must be_==(700L)
          sp2
        } --> { sp =>
          sp.toRegionSet.size must be_==(700L)
          sp.toRegionSet.alloc(600).isDefined must be_==(true)
          sp.toRegionSet.alloc(800).isDefined must be_==(false)
        }
    }
  }

  "A Graph Search" should {
    implicit def mapToSearch(g : Map[Int,sciList[Int]]) : GraphSearch[Int,Set[Int]] = new SetSearch(g)
    "Correctly search a directed graph without loops" in {
      Map(1 -> sciList(2,3), 2 -> sciList(3)).getReachable(1) must be_==(Set(1,2,3))
      Map(1 -> sciList(2,3), 2 -> sciList(3)).getReachable(2) must be_==(Set(2,3))
      Map(1 -> sciList(2,3), 2 -> sciList(3)).getReachable(3) must be_==(Set(3))
    }
    "Correctly search a directed graph with loops" in {
      Map(1 -> sciList(2,3), 2 -> sciList(3), 3 -> sciList(1)).getReachable(1) must be_==(Set(1,2,3))
      Map(1 -> sciList(2,3), 2 -> sciList(3), 3 -> sciList(1)).getReachable(2) must be_==(Set(1,2,3))
      Map(1 -> sciList(2,3), 2 -> sciList(3), 3 -> sciList(1)).getReachable(3) must be_==(Set(1,2,3))
    }
  }
}
