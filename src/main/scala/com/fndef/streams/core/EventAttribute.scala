package com.fndef.streams.core

import java.util.Date

sealed abstract class EventAttribute extends Ordered[EventAttribute] with EventAttributeOrdering {
  type AttrType
  val name: String
  val value: AttrType

  def valueAs[T](implicit fn: EventAttribute => T): T = fn(this)

  def as(nameAs: String): EventAttribute

  assert(name != null, "Null attribute name")

  def hasValue(): Boolean = {
    Option(value).nonEmpty
  }

  override def compare(that: EventAttribute): Int = {
    compareAttributes(this, that)
  }
}

object EventAttribute {
  def apply(name: String, value: Int): EventAttribute = {
    IntAttribute(name, value)
  }

  def apply(name: String, value: Byte): EventAttribute = {
    ByteAttribute(name, value)
  }

  def apply(name: String, value: Short): EventAttribute = {
    ShortAttribute(name, value)
  }

  def apply(name: String, value: Long): EventAttribute = {
    LongAttribute(name, value)
  }

  def apply(name: String, value: Float): EventAttribute = {
    FloatAttribute(name, value)
  }

  def apply(name: String, value: Double): EventAttribute = {
    DoubleAttribute(name, value)
  }

  def apply(name: String, value: String): EventAttribute = {
    StringAttribute(name, value)
  }

  def apply(name: String, value: Boolean): EventAttribute = {
    BoolAttribute(name, value)
  }

  def apply(name: String, value: BigDecimal): EventAttribute = {
    BigDecimalAttribute(name, value)
  }

  def apply(name: String, value: Date): EventAttribute = {
    DateAttribute(name, value)
  }

  def apply(name: String, value: Array[Byte]): EventAttribute = {
    ByteArrayAttribute(name, value)
  }

  def apply(name: String, value: Null): EventAttribute = {
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


