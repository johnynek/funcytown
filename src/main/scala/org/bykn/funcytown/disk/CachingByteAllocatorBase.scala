package org.bykn.funcytown.disk

import org.bykn.funcytown.LRUMap

// Caches the actual objects, not the bytes read
abstract class CachingByteAllocatorBase(cachedItems : Int) extends ByteAllocator {

  protected val cache = new LRUMap[Long,AnyRef](cachedItems)

  override def deref[T](ptr : Long) = {
    val cached = cache.synchronized { cache.get(ptr) }
    if (cached == null) {
      //We need to actually read off disk:
      val toCache = super.deref[T](ptr)
      cache.synchronized { cache.put(ptr, toCache.asInstanceOf[AnyRef]) }
      toCache
    }
    else {
      // return the cached value:
      cached
    }.asInstanceOf[T]
  }

  override def afterAlloc[T](ptr : Long, obj : T) : T = {
    // Make sure the allocation is in the read cache, since writing is async
    cache.synchronized { cache.put(ptr, obj.asInstanceOf[AnyRef]) }
    // We are the first sublass under ByteAllocator, so we are done with the afterAlloc chain:
    obj
  }
}
