package org.bykn.funcytown.disk

import org.bykn.funcytown.io._
import org.bykn.funcytown.storage.FileStorage
import org.bykn.funcytown.SyncVar

class DiskAllocator(filename : String, spillSize : Int)
  extends ByteAllocator with FileStorage {
  override val file = new SyncVar[FileLike](FileLikeSeq(MemoryFileLike(spillSize),
    RandAccessFileLike(filename)))
  // initialize the file:
  this.init

  override def finalize = close
}
