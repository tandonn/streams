package com.fndef.streams.core

import java.util.Date

import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers

class EventAttributeSpec extends AnyFlatSpec with Matchers {
  "A byte attribute" should "have byte value" in {
    val attr: EventAttribute = EventAttribute("attr", 5.toByte)
    assert(attr.isInstanceOf[ByteAttribute], "Attribute is not byte attribute")
    assert(attr.name == "attr", "Attribute name not correct")
    assert(attr.hasValue(), "Attribute has no value")
    assert(attr.value.isInstanceOf[Byte], "Attribute is not byte")
    assert(attr.value == 5, "Incorrect attribute value")
  }

  "A short attribute" should "have short value" in {
    val attr: EventAttribute = EventAttribute("attr", 300.toShort)
    assert(attr.isInstanceOf[ShortAttribute], "Attribute is not short attribute")
    assert(attr.name == "attr", "Attribute name not correct")
    assert(attr.hasValue(), "Attribute has no value")
    assert(attr.value.isInstanceOf[Short], "Attribute is not short")
    assert(attr.value == 300, "Incorrect attribute value")
  }

  "An int attribute" should "have int value" in {
    val attr: EventAttribute = EventAttribute("intAttr", 10)
    assert(attr.isInstanceOf[IntAttribute], "Attribute is not int attribute")
    assert(attr.name == "intAttr", "Attribute name not correct")
    assert(attr.hasValue(), "Attribute has no value")
    assert(attr.value.isInstanceOf[Int], "Attribute is not Int")
    assert(attr.value == 10, "Incorrect attribute value")
  }

  "A long attribute" should "have long value" in {
    val attr: EventAttribute = EventAttribute("attr", 100L)
    assert(attr.isInstanceOf[LongAttribute], "Attribute is not long attribute")
    assert(attr.name == "attr", "Attribute name not correct")
    assert(attr.hasValue(), "Attribute has no value")
    assert(attr.value.isInstanceOf[Long], "Attribute is not long")
    assert(attr.value == 100L, "Incorrect attribute value")
  }

  "A float attribute" should "have float value" in {
    val attr: EventAttribute = EventAttribute("attr", 100.12f)
    assert(attr.isInstanceOf[FloatAttribute], "Attribute is not float attribute")
    assert(attr.name == "attr", "Attribute name not correct")
    assert(attr.hasValue(), "Attribute has no value")
    assert(attr.value.isInstanceOf[Float], "Attribute is not float")
    assert(attr.value == 100.12f, "Incorrect attribute value")
  }

  "A double attribute" should "have double value" in {
    val attr: EventAttribute = EventAttribute("attr", 100.12d)
    assert(attr.isInstanceOf[DoubleAttribute], "Attribute is not double attribute")
    assert(attr.name == "attr", "Attribute name not correct")
    assert(attr.hasValue(), "Attribute has no value")
    assert(attr.value.isInstanceOf[Double], "Attribute is not double")
    assert(attr.value == 100.12d, "Incorrect attribute value")
  }

  "A string attribute" should "have string value" in {
    val attr: EventAttribute = EventAttribute("attr", "hello")
    assert(attr.isInstanceOf[StringAttribute],"Attribute is not string attribute")
    assert(attr.name == "attr", "Attribute name not correct")
    assert(attr.hasValue(), "Attribute has no value")
    assert(attr.value.isInstanceOf[String], "Attribute is not string")
    assert(attr.value == "hello", "Incorrect attribute value")
  }

  "A date attribute" should "have date value" in {
    val dt: Date = new Date()
    val attr: EventAttribute = EventAttribute("attr", dt)
    assert(attr.isInstanceOf[DateAttribute], "Attribute is not date attribute")
    assert(attr.name == "attr", "Attribute name not correct")
    assert(attr.hasValue(), "Attribute has no value")
    assert(attr.value.isInstanceOf[Date], "Attribute is not date")
    assert(attr.value == dt, "Incorrect attribute value")
  }

  "A big decimal attribute" should "have big decimal value" in {
    val bd: BigDecimal = BigDecimal(100.1232)
    val attr: EventAttribute = EventAttribute("attr", bd)
    assert(attr.isInstanceOf[BigDecimalAttribute], "Attribute is not big decimal attribute")
    assert(attr.name == "attr", "Attribute name not correct")
    assert(attr.hasValue(), "Attribute has no value")
    assert(attr.value.isInstanceOf[BigDecimal], "Attribute is not big decimal")
    assert(bd.compare(attr.value.asInstanceOf[BigDecimal]) == 0, "Incorrect attribute value")
  }

  "A byte array attribute" should "have byte array value" in {
    val arr: Array[Byte] = Array(1.toByte, 2.toByte)
    val attr: EventAttribute = EventAttribute("attr", arr)
    assert(attr.isInstanceOf[ByteArrayAttribute], "Attribute is not byte array attribute")
    assert(attr.name == "attr", "Attribute name not correct")
    assert(attr.hasValue(), "Attribute has no value")
    assert(attr.value.isInstanceOf[Array[Byte]], "Attribute is not byte array")
    assert(arr.sameElements(attr.value.asInstanceOf[Array[Byte]]), "Incorrect attribute value")
  }

  "A null value attribute" should "not have a value" in {
    val attr: EventAttribute = EventAttribute("attr", null)
    assert(attr.isInstanceOf[NoValueAttribute], "Attribute is not no value attribute")
    assert(attr.name == "attr", "Attribute name not correct")
    assert(!attr.hasValue(), "Attribute value is not null")
    assert(attr.value == null, "Attribute has a value")
  }

  "An event attribute" should " be equal to an attribute with the same name, value and type" in {
    val a1: EventAttribute = EventAttribute("attr1", 10)
    val a2: EventAttribute = EventAttribute("attr1", 10)
    assert(a1 == a2, "Attributes are different")
  }

  "Attributes with the same name but different values" should "be different" in {
    val a1: EventAttribute = EventAttribute("attr1", 100)
    val a2: EventAttribute = EventAttribute("attr1", 10)
    assert(a1 != a2, "Attributes are same")
  }

  "Attributes with the same name and values but different types" should "be different" in {
    val a1: EventAttribute = EventAttribute("attr1", 10.toByte)
    val a2: EventAttribute = EventAttribute("attr1", 10)
    assert(a1 != a2, "Attributes are same")
  }
}