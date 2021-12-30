package com.fndef.streams.core

trait EventAttributeOrdering {
  import com.fndef.streams.core.Implicits._
  def compareAttributes(thisAttr: EventAttribute, thatAttr: EventAttribute): Int = {
    thisAttr match {
      case attr: ByteAttribute =>
        compareValues(attr.valueAs[Byte], thatAttr)
      case attr: ShortAttribute =>
        compareValues(attr.valueAs[Short], thatAttr)
      case attr: IntAttribute =>
        compareValues(attr.valueAs[Int], thatAttr)
      case attr: LongAttribute =>
        compareValues(attr.valueAs[Long], thatAttr)
      case attr: FloatAttribute =>
        compareValues(attr.valueAs[Float], thatAttr)
      case attr: DoubleAttribute =>
        compareValues(attr.valueAs[Double], thatAttr)
      case attr: StringAttribute =>
        compareValues(attr.valueAs[String], thatAttr)
      case attr: BoolAttribute =>
        compareValues(attr.valueAs[Boolean], thatAttr)
      case attr: BigDecimalAttribute =>
        compareValues(attr.valueAs[BigDecimal].toDouble, thatAttr)
      case _ =>
        thisAttr.compare(thatAttr)

    }
  }

  private def compareValues(v1: Boolean, attr: EventAttribute): Int = {
    attr match {
      case a: BoolAttribute =>
        v1.compare(a.valueAs[Boolean])
      case _: NoValueAttribute =>
        1
      case _ =>
        v1.toString.compare(attr.value.toString)
    }
  }

  private def compareValues(v1: String, attr: EventAttribute): Int = {
    attr match {
      case a: StringAttribute =>
        v1.compare(a.valueAs[String])
      case a: ByteAttribute =>
        v1.compare(a.valueAs[Byte].toString)
      case a: ShortAttribute =>
        v1.compare(a.valueAs[Short].toString)
      case a: IntAttribute =>
        v1.compare(a.valueAs[Int].toString)
      case a: LongAttribute =>
        v1.compare(a.valueAs[Long].toString)
      case a: FloatAttribute =>
        v1.compare(a.valueAs[Float].toString)
      case a: DoubleAttribute =>
        v1.compare(a.valueAs[Double].toString)
      case a: BoolAttribute =>
        v1.compare(a.valueAs[Boolean].toString)
      case a: BigDecimalAttribute =>
        v1.compare(a.valueAs[BigDecimal].toString)
      case _: NoValueAttribute =>
        1
      case _ =>
        v1.compare(attr.value.toString)
    }
  }

  private def compareValues(v1: Long, attr: EventAttribute): Int = {
    attr match {
      case a: ByteAttribute =>
        v1.compare(a.valueAs[Byte])
      case a: ShortAttribute =>
        v1.compare(a.valueAs[Short])
      case a: IntAttribute =>
        v1.compare(a.valueAs[Int])
      case a: LongAttribute =>
        v1.compare(a.valueAs[Long])
      case a: FloatAttribute =>
        v1.toFloat.compare(a.valueAs[Float])
      case a:DoubleAttribute =>
        v1.toDouble.compare(a.valueAs[Double])
      case a: StringAttribute =>
        v1.toString.compare(a.valueAs[String])
      case _: NoValueAttribute =>
        1
      case _ =>
        v1.toString.compare(attr.value.toString)
    }
  }

  private def compareValues(v1: Double, attr: EventAttribute): Int = {
    attr match {
      case a: ByteAttribute =>
        v1.compare(a.valueAs[Byte])
      case a: ShortAttribute =>
        v1.compare(a.valueAs[Short])
      case a: IntAttribute =>
        v1.compare(a.valueAs[Int])
      case a: LongAttribute =>
        v1.compare(a.valueAs[Long])
      case a: FloatAttribute =>
        v1.compare(a.valueAs[Float])
      case a: DoubleAttribute =>
        v1.compareTo(a.valueAs[Double])
      case a: StringAttribute =>
        v1.toString.compare(a.valueAs[String])
      case _: NoValueAttribute =>
        1
      case _ =>
        v1.toString.compare(attr.value.toString)
    }
  }
}
