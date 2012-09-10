package org.bykn.funcytown

import java.util.{Map => JMap, LinkedHashMap}

class LRUMap[K,V](maxEntries : Int) extends LinkedHashMap[K,V](maxEntries + 1, 1.0f, true) {
  override protected def removeEldestEntry(eldest : JMap.Entry[K,V]) = size > maxEntries
}
