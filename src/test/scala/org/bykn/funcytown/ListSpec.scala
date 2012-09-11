package org.bykn.funcytown

import java.util.Random

import org.bykn.funcytown.gc.GCDiskAllocator
import org.specs._

class ListSpec extends Specification {
  noDetailedDiffs()
  // Cache the last N objects, so we have to hit disk sometimes
  //implicit val alloc = new CachingDiskAllocator(50)
  implicit val alloc = new GCDiskAllocator(1 << 16) //64k
  //implicit val alloc = new DiskAllocator("test", 1 << 16) // 64k of ram
  //implicit val alloc = new MemoryAllocator
  val ITERS = 100

  "A funcytown.List" should {
    "look like immutable.List" in {
      try {
      // Be sure to use the right object:
      val r = new Random
      // Make the initial lists:
      var flist = org.bykn.funcytown.List(0)
      // Damn type erasure!
      val fclass = flist.getClass.asInstanceOf[AnyRef]
      var ilist = scala.collection.immutable.List(0)
      flist.size must be_==(ilist.size)
      (1 to ITERS).foreach { iter =>
        // Make sure no methods inadvertantly convert to another List subclass
        (flist.getClass.asInstanceOf[AnyRef]) must be_==(fclass)
        // Usually add (3/4 of the time)
        if (r.nextBoolean || r.nextBoolean) {
          val value = r.nextInt
          ilist = value :: ilist
          flist = value :: flist
          flist.indexOf(value) must be_==(flist.indexOf(value))
        }
        else if(!ilist.isEmpty) {
          val last = ilist.last
          ilist = ilist.filter { _ != last }
          flist = flist.filter { _ != last }
          // They should be equal
          ilist.zip(flist).forall { tup => tup._1 == tup._2 } must be_==(true)
        }
        else {
          flist.isEmpty must be_==(true)
        }
        flist.size must be_==(ilist.size)
      }
      }
      catch {
        case t : Throwable => { t.printStackTrace; throw t }
      }
    }
  }
}
