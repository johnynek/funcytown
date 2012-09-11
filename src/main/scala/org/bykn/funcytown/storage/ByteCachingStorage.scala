package org.bykn.funcytown.storage
import org.bykn.funcytown.LRUMap

trait ByteCachingStorage extends AsyncWriterStorage {
  // write into a cache, use akka to have an actor own the cache
  // read from the cache, on misses hit file, fill cache
  val cachedItems : Int

  protected val cache = new LRUMap[Long,(Byte, Array[Byte])](cachedItems)

  override def readBytes(ptr : Long) : (Byte, Array[Byte]) = {
    val cached = cache.synchronized { cache.get(ptr) }
    if (cached == null) {
      //We need to actually read off disk:
      val toCache = super.readBytes(ptr)
      cache.synchronized { cache.put(ptr, toCache) }
      toCache
    }
    else {
      // return the cached value:
      cached
    }
  }

  override def writeBytes(objType : Byte, toWrite : Array[Byte]) : Long = {
    val ptr = super.writeBytes(objType, toWrite)
    cache.synchronized { cache.put(ptr, (objType, toWrite)) }
    ptr
  }

}
