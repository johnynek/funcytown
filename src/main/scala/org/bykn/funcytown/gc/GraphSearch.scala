package org.bykn.funcytown.gc

import scala.collection.immutable.Queue
import scala.annotation.tailrec

abstract class GraphSearch[N,V] {
  def empty : V
  def visit(node: N, visited : V) : V
  def haveVisited(node : N, visited : V) : Boolean
  def neighborsOf(from : N) : Iterable[N]

  def getReachable(from : N) : V = breadthFirst(Queue(from), empty)

  @tailrec
  final def breadthFirst(toVisit : Queue[N], visited : V = empty, toVisitSet : Set[N] = null) : V = {
    if (toVisit.isEmpty) {
      visited
    }
    else {
      val inToVisit = Option(toVisitSet).getOrElse(toVisit.toSet)
      val (next, rest) = toVisit.dequeue
      val newVisited = visit(next, visited)
      val newNodes = neighborsOf(next).filter { node =>
        !haveVisited(node, newVisited) && !inToVisit(node)
      }
      breadthFirst(rest ++ newNodes, newVisited, inToVisit ++ newNodes)
    }
  }
}

