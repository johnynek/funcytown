package com.twitter.funcytown

import com.esotericsoftware.kryo.io.{Input => KInput}
import com.esotericsoftware.kryo.io.{Output => KOutput}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}

import scala.collection.immutable.{List => sciList}

// Using the collection builder stuff, I bet you can do this correctly for anything with a builder
class ScalasciListSerializer[T] extends KSerializer[sciList[T]] {
  override def write(k : Kryo, out : KOutput, obj : sciList[T]) {
    out.writeInt(obj.size, true)
    obj.foreach { item =>
      k.writeClassAndObject(out, item)
    }
  }

  override def create(k : Kryo, in : KInput, ltype : Class[sciList[T]]) : sciList[T] = {
    val sz = in.readInt(true)
    (0 until sz).foldLeft(sciList[T]()) { (oldsciList, idx) =>
      k.readClassAndObject(in).asInstanceOf[T] :: oldsciList
    }.reverse // reverse the stack to get the original order
  }
}

class HashEntrySerializer[K,V](hashfn : K => Long) extends KSerializer[HashEntry[K,V]] {
  override def write(k : Kryo, out : KOutput, obj : HashEntry[K,V]) {
    k.writeClassAndObject(out, obj.key.asInstanceOf[AnyRef])
    k.writeClassAndObject(out, obj.value.asInstanceOf[AnyRef])
  }
  override def create(k : Kryo, in : KInput, clazz : Class[HashEntry[K,V]]) : HashEntry[K,V] = {
    val key = k.readClassAndObject(in).asInstanceOf[K]
    val value = k.readClassAndObject(in).asInstanceOf[V]
    new HashEntry(hashfn(key), key, value)
  }
}

class SingletonSerializer[T](inst : T) extends KSerializer[T] {
  override def write(k : Kryo, out : KOutput, obj : T) {
    assert(inst == obj, "Singleton serializer only works for one instance")
    // Do nothing
  }
  override def create(k : Kryo, in : KInput, stype : Class[T]) : T = inst
}

class IdnSerializer[T <: ImmutableDagNode[Long]](mem : ByteAllocator) extends KSerializer[T] {
  override def write(k : Kryo, out : KOutput, obj : T) {
    // ptr is always positive:
    assert(mem == obj.mem, "Can only serialize into the same allocator")
    // Just write the pointer:
    out.writeLong(obj.selfPtr, true)
  }
  override def create(k : Kryo, in : KInput, clazz : Class[T]) : T = {
    mem.deref[T](in.readLong(true))
  }
}
