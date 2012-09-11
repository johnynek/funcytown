package org.bykn.funcytown.disk

import org.bykn.funcytown.storage._
import org.bykn.funcytown.io._
import org.bykn.funcytown.SyncVar

// Keep 10MB in memory before going to disk by default
class CachingDiskAllocator(cachedItems : Int, spillSize : Int = 10000000, filename : String = null)
  extends CachingByteAllocatorBase(cachedItems) with AsyncWriterStorage {

  // TODO check file.deleteOnExit() to make sure this gets cleaned up
  private val realFileName = Option(filename).getOrElse(java.util.UUID.randomUUID.toString)
  override val file = new SyncVar[FileLike](FileLikeSeq(MemoryFileLike(spillSize),
    RandAccessFileLike(realFileName)))

  // initialize the file, start the write thread
  this.init

  override def finalize { close }

  override def close {
    super.close
    // Delete the file if we need to:
    if (filename == null) {
      (new java.io.File(realFileName)).delete
    }
  }
}
