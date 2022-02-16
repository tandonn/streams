package com.fndef.streams.core

import java.util.Date

sealed abstract class EventAttribute extends Ordered[EventAttribute] {
  type AttrType
  val name: String
  val value: AttrType

  def valueAs[T](implicit fn: EventAttribute => T): T = fn(this)

  def as(nameAs: String): EventAttribute

  assert(name != null, "Null attribute name")

  def hasValue(): Boolean = {
    Option(value).nonEmpty
  }

  def compare(that: EventAttribute): Int = {
    (this, that) match {
      case (b1: ByteAttribute, b2: ByteAttribute) =>
        b1.value.compare(b2.value)
      case (s1: ShortAttribute, s2: ShortAttribute) =>
        s1.value.compare(s2.value)
      case (i1: IntAttribute, i2: IntAttribute) =>
        i1.value.compare(i2.value)
      case (l1: LongAttribute, l2: LongAttribute) =>
        l1.value.compare(l2.value)
      case (f1: FloatAttribute, f2: FloatAttribute) =>
        f1.value.compare(f2.value)
      case (d1: DoubleAttribute, d2: DoubleAttribute) =>
        d1.value.compare(d2.value)
      case (bool1: BoolAttribute, bool2: BoolAttribute) =>
        bool1.value.compare(bool2.value)
      case (str1: StringAttribute, str2: StringAttribute) =>
        str1.value.compare(str2.value)
      case (bd1: BigDecimalAttribute, bd2: BigDecimalAttribute) =>
        bd1.value.compare(bd2.value)
      case (dt1: DateAttribute, dt2: DateAttribute) =>
        dt1.value.compareTo(dt2.value)
      case (n1: NoValueAttribute, n2: NoValueAttribute) =>
        0
      case (ba1:  ByteArrayAttribute, ba2: ByteArrayAttribute) =>
        if (ba1.value.length > ba2.value.length) {
          1
        } else if (ba1.value.length < ba2.value.length) {
          -1
        } else 0
      case _ => throw new IllegalArgumentException(s"Can't be compared [${this} with ${that}]")
    }
  }
}

object EventAttribute {
  def apply(name: String, value: Int): IntAttribute = {
    IntAttribute(name, value)
  }

  def apply(name: String, value: Byte): ByteAttribute = {
    ByteAttribute(name, value)
  }

  def apply(name: String, value: Short): ShortAttribute = {
    ShortAttribute(name, value)
  }

  def apply(name: String, value: Long): LongAttribute = {
    LongAttribute(name, value)
  }

  def apply(name: String, value: Float): FloatAttribute = {
    FloatAttribute(name, value)
  }

  def apply(name: String, value: Double): DoubleAttribute = {
    DoubleAttribute(name, value)
  }

  def apply(name: String, value: String): StringAttribute = {
    StringAttribute(name, value)
  }

  def apply(name: String, value: Boolean): BoolAttribute = {
    BoolAttribute(name, value)
  }

  def apply(name: String, value: BigDecimal): BigDecimalAttribute = {
    BigDecimalAttribute(name, value)
  }

  def apply(name: String, value: Date): DateAttribute = {
    DateAttribute(name, value)
  }

  def apply(name: String, value: Array[Byte]): ByteArrayAttribute = {
    ByteArrayAttribute(name, value)
  }

  def apply(name: String, value: Null): NoValueAttribute = {
    NoValueAttribute(name, value)
  }
}

case class NoValueAttribute(name: String, value: Null) extends EventAttribute {
  type AttrType = Null

  def as(nameAs: String): EventAttribute = {
    NoValueAttribute(nameAs, value)
  }
}

case class IntAttribute(name: String, value: Int) extends EventAttribute {
  type AttrType = Int

  def as(nameAs: String): EventAttribute = {
    IntAttribute(nameAs, value)
  }
}

case class LongAttribute(name: String, value: Long) extends EventAttribute {
  type AttrType = Long

  def as(nameAs: String): EventAttribute = {
    LongAttribute(nameAs, value)
  }
}

case class ShortAttribute(name: String, value: Short) extends EventAttribute {
  type AttrType = Short

  def as(nameAs: String): EventAttribute = {
    ShortAttribute(nameAs, value)
  }
}

case class ByteAttribute(name: String, value: Byte) extends EventAttribute {
  type AttrType = Byte

  def as(nameAs: String): EventAttribute = {
    ByteAttribute(nameAs, value)
  }
}

case class FloatAttribute(name: String, value: Float) extends EventAttribute {
  type AttrType = Float

  def as(nameAs: String): EventAttribute = {
    FloatAttribute(nameAs, value)
  }
}

case class DoubleAttribute(name: String, value: Double) extends EventAttribute {
  type AttrType = Double

  def as(nameAs: String): EventAttribute = {
    DoubleAttribute(nameAs, value)
  }
}

case class BoolAttribute(name: String, value: Boolean) extends EventAttribute {
  type AttrType = Boolean

  def as(nameAs: String): EventAttribute = {
    BoolAttribute(nameAs, value)
  }
}

case class StringAttribute(name: String, value: String) extends EventAttribute {
  type AttrType = String

  def as(nameAs: String): EventAttribute = {
    StringAttribute(nameAs, value)
  }
}

case class BigDecimalAttribute(name: String, value: BigDecimal) extends EventAttribute {
  type AttrType = BigDecimal

  def as(nameAs: String): EventAttribute = {
    BigDecimalAttribute(nameAs, value)
  }
}

case class DateAttribute(name: String, value: Date) extends EventAttribute {
  type AttrType = Date

  def as(nameAs: String): EventAttribute = {
    DateAttribute(nameAs, value)
  }
}

case class ByteArrayAttribute(name: String, value: Array[Byte]) extends EventAttribute {
  type AttrType = Array[Byte]

  def as(nameAs: String): EventAttribute = {
    ByteArrayAttribute(nameAs, value)
  }
}
