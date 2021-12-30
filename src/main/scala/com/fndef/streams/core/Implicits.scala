package com.fndef.streams.core

import java.util.UUID


object Implicits {

  implicit val processingContext: ProcessingContext = ProcessingContext()
  implicit def toByte(eventAttribute: EventAttribute): Byte = eventAttribute.value.asInstanceOf[Byte]
  implicit def toShort(eventAttribute: EventAttribute): Short = eventAttribute.value.asInstanceOf[Short]
  implicit def toInt(eventAttribute: EventAttribute): Int = eventAttribute.value.asInstanceOf[Int]
  implicit def toLong(eventAttribute: EventAttribute): Long = eventAttribute.value.asInstanceOf[Long]
  implicit def toFloat(eventAttribute: EventAttribute): Float = eventAttribute.value.asInstanceOf[Float]
  implicit def toDouble(eventAttribute: EventAttribute): Double = eventAttribute.value.asInstanceOf[Double]
  implicit def toBool(eventAttribute: EventAttribute): Boolean = eventAttribute.value.asInstanceOf[Boolean]
  implicit def toByteArray(eventAttribute: EventAttribute): Array[Byte] = eventAttribute.value.asInstanceOf[Array[Byte]]
  implicit def toBigDecimal(eventAttribute: EventAttribute): BigDecimal = eventAttribute.value.asInstanceOf[BigDecimal]
  implicit def toStr(eventAttribute: EventAttribute): String = eventAttribute.value.asInstanceOf[String]
  implicit val registerOp = (sink: StreamSink, source: StreamSource) => source.registerSink(sink)
  implicit val nameGenerator = (prefix: String) => s"prefix-${UUID.randomUUID().toString}"
}