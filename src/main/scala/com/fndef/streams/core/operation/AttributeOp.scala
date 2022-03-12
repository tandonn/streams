package com.fndef.streams.core.operation

import com.fndef.streams.core.{EventAttribute, EventInternal, ProcessingContext}
import com.fndef.streams.core.Implicits._

sealed trait AttributeOp {
  val attributeName: String
  val opName: String
  def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute
}

trait AggregateOp extends AttributeOp {
  val aggregationName: String
}

object AggregationOpPatterns {
  val countRE = """count\s*[(](.*)[)]\s*""".r
  val minRE = """min\s*[(](.*)[)]\s*""".r
  val maxRE = """max\s*[(](.*)[)]\s*""".r
  val avgRE = """avg\s*[(](.*)[)]\s*""".r
  val sumRE = """sum\s*[(](.*)[)]\s*""".r
}

object AggregateOp {

  import AggregationOpPatterns._

  def apply(attribute: String): AggregateOp = {
    attribute.trim.toLowerCase match {
      case countRE(attributeName) =>
        CountOp(attributeName.toLowerCase)
      case sumRE(attributeName) =>
        SumOp(attributeName.toLowerCase)
      case minRE(attributeName) =>
        MinOp(attributeName.toLowerCase)
      case maxRE(attributeName) =>
        MaxOp(attributeName.toLowerCase)
      case avgRE(attributeName) =>
        AvgOp(attributeName.toLowerCase)
      // case litRE(literal) =>
      //  LiteralOp(literal)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported aggregation - ${attribute}")
    }
  }

  def isAggregation(attribute: String): Boolean = {
    attribute.trim.toLowerCase match {
      case countRE(_) | sumRE(_) | minRE(_) | maxRE(_) | avgRE(_) =>
        true
      case _ =>
        false
    }
  }
}

case class CountOp(attributeName: String) extends AggregateOp {
  val aggregationName: String = "count"
  val opName: String = s"${aggregationName}(${attributeName.trim})"
  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    eventInternal.getAttribute(attributeName) match {
      case Some(_) =>
        val count: Long = context.groupingContext.getAttribute(eventInternal, opName).map(_.valueAs[Long]).getOrElse(0)
        context.groupingContext.addAttribute(eventInternal, EventAttribute(opName, count+1))
      case None =>
        context.groupingContext.getAttribute(eventInternal, opName).getOrElse(EventAttribute(opName, 0))
    }
  }
}

case class SumOp(attributeName: String) extends AggregateOp {
  val aggregationName: String = "sum"
  val opName: String = s"${aggregationName}(${attributeName.trim})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    eventInternal.getAttribute(attributeName) match {
      case Some(attr) =>
        val sum: Double = context.groupingContext.getAttribute(eventInternal, opName).map(_.valueAs[Double]).getOrElse(0.0)
        context.groupingContext.addAttribute(eventInternal, EventAttribute(opName, (sum+attr.valueAs[Double])))
      case None =>
        context.groupingContext.getAttribute(eventInternal, opName).getOrElse(EventAttribute(opName, 0.0))
    }
  }
}

case class AvgOp(attributeName: String) extends AggregateOp {
  override val aggregationName: String = "avg"
  val opName: String = s"${aggregationName}(${attributeName.trim})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    eventInternal.getAttribute(attributeName) match {
      case Some(attr) =>
        val avg: Double = context.groupingContext.getAttribute(eventInternal, opName).map(_.valueAs[Double]).getOrElse(0.0)
        val count: Int = context.groupingContext.getAttribute(eventInternal, s"count(${attributeName})").map(_.valueAs[Int]).getOrElse(0)
        context.groupingContext.addAttribute(eventInternal, EventAttribute(opName, ((avg*count)+attr.valueAs[Double]) / (count+1)))
        context.groupingContext.addAttribute(eventInternal, EventAttribute(s"count(${attributeName})", count+1))
      case None =>
        context.groupingContext.getAttribute(eventInternal, opName).getOrElse(EventAttribute(opName, 0.0))
    }
  }
}

case class MaxOp(attributeName: String) extends AggregateOp {
  val aggregationName: String = "max"
  val opName: String = s"${aggregationName}(${attributeName.trim})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    val contextVal: Option[EventAttribute] = context.groupingContext.getAttribute(eventInternal, opName)
    val eventVal: Option[EventAttribute] = eventInternal.getAttribute(attributeName)

    contextVal match {
      case Some(cv) =>
        eventVal match {
          case Some(ev) if ev.compare(cv) > 0 =>
            context.groupingContext.addAttribute(eventInternal, ev.as(opName))
          case _ =>
            context.groupingContext.addAttribute(eventInternal, cv.as(opName))
        }
      case _ =>
        eventVal match {
          case Some(ev) =>
            context.groupingContext.addAttribute(eventInternal, ev.as(opName))
          case _ =>
            context.groupingContext.addAttribute(eventInternal, EventAttribute(opName, null))
        }
    }
  }
}

case class MinOp(attributeName: String) extends AggregateOp {
  val aggregationName: String = "min"
  val opName: String = s"${aggregationName}(${attributeName.trim})"
  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    val contextVal: Option[EventAttribute] = context.groupingContext.getAttribute(eventInternal, attributeName)
    val eventVal: Option[EventAttribute] = eventInternal.getAttribute(attributeName)

    contextVal match {
      case Some(cv) =>
         eventVal match {
           case Some(ev) if ev.compare(cv) < 0 =>
             context.groupingContext.addAttribute(eventInternal, ev.as(opName))
           case _ =>
             context.groupingContext.addAttribute(eventInternal, cv.as(opName))
         }
      case _ =>
        eventVal match {
          case Some(ev) =>
            context.groupingContext.addAttribute(eventInternal, ev.as(opName))
          case _ =>
            context.groupingContext.addAttribute(eventInternal, EventAttribute(opName, null))
        }
    }
  }
}

case class SelectOp(attributeName: String) extends AttributeOp {
  val opName: String = attributeName.trim
  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    eventInternal.getAttribute(attributeName.trim) match {
      case Some(attr) =>
        attr
      case None =>
        EventAttribute(opName, null)
    }
  }
}

trait LiteralOp extends AttributeOp {
  val litRE = """lit\s*[(](.*)[)]\s*""".r

  def isLiteral(attribute: String): Boolean = {
    attribute.trim.toLowerCase match {
      case litRE(_) =>
        true
      case _ =>
        false
    }
  }
}

object LiteralOp {
  def apply(value: Int): LiteralOp = IntLiteralOp(value)

  def apply(value: Long): LiteralOp = LongLiteralOp(value)

  def apply(value: Float): LiteralOp = FloatLiteralOp(value)

  def apply(value: Double): LiteralOp = DoubleLiteralOp(value)

  def apply(value: String): LiteralOp = StringLiteralOp(value)

  def apply(value: Boolean): LiteralOp = BoolLiteralOp(value)
}

case class IntLiteralOp(value: Int) extends LiteralOp {
  override val attributeName: String = s"lit(${value})"
  override val opName: String = s"lit(${value})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

case class LongLiteralOp(value: Long) extends LiteralOp {
  override val attributeName: String = s"lit(${value})"
  override val opName: String = s"lit(${value})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

case class FloatLiteralOp(value: Float) extends LiteralOp {
  override val attributeName: String = s"lit(${value})"
  override val opName: String = s"lit(${value})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

case class DoubleLiteralOp(value: Double) extends LiteralOp {
  override val attributeName: String = s"lit(${value})"
  override val opName: String = s"lit(${value})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

case class BoolLiteralOp(value: Boolean) extends LiteralOp {
  override val attributeName: String = s"lit(${value})"
  override val opName: String = s"lit(${value})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

case class StringLiteralOp(value: String) extends LiteralOp {
  override val attributeName: String = s"lit(${value})"
  override val opName: String = s"lit(${value})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}
