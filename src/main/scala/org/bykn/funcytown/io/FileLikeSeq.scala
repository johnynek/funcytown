package org.bykn.funcytown.io

import scala.collection.immutable.{List => sciList}

object FileLikeSeq {
  def apply(filelike : FileLike*) = {
    new FileLikeSeq(sciList(filelike : _*), sciList[FileLike](), 0L)
  }
}

/**
 * This is how we handle spilling: we first put a MemoryFileLike followed
 * by a RandFileLike, so we can go to memory first.  In principle
 * we could layer deeper and deeper if we needed (maybe a file on SSD, then HD, then
 * network disk)
 */
class FileLikeSeq(next : sciList[FileLike], passed : sciList[FileLike],
  override val position : Long) extends FileLike {

  override def close = {
    next.foreach { _.close }
    passed.foreach { _.close }
    this
  }

  override def toHead = FileLikeSeq {
    passed
      //Push all the passed back into next
      .foldLeft(next) { (old, fl) => fl :: old }
      //go back to head
      .map { _.toHead } : _*
    }

  protected def doRight(amt : Long)(action : (FileLike) => (Long, FileLike)) : (Long,FileLike) = {
    next.headOption.map { nextHead =>
      val movedNextFL = action(nextHead)
      val moved = movedNextFL._1
      val (nn, np) = if (moved == amt) {
        // next.head could accomodate
        val newNext = movedNextFL._2 :: next.tail
        (newNext, passed)
      }
      else {
        //We exhausted the head:
        (next.tail, movedNextFL._2 :: passed)
      }
      (moved, new FileLikeSeq(nn, np, position + moved))
    }
    .getOrElse {
      (0L, this)
    }
  }
  override def moveRight(right : Long) = doRight(right) { _.moveRight(right) }
  override def read(into : Array[Byte], offset : Int, size : Int) = doRight(size) {
    _.read(into, offset, size)
  }
  override def write(into : Array[Byte], offset : Int, size : Int) = doRight(size) {
    _.write(into, offset, size)
  }
}

