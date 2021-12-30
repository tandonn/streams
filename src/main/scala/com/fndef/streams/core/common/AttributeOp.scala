package com.fndef.streams.core.common

import com.fndef.streams.core.{ContextGroup, EventAttribute, EventInternal, ProcessingContext}
import com.fndef.streams.core.Implicits._
object AggregationOpPatterns {
  val countRE = """count\s*[(](.*)[)]\s*""".r
  val minRE = """min\s*[(](.*)[)]\s*""".r
  val maxRE = """max\s*[(](.*)[)]\s*""".r
  val avgRE = """avg\s*[(](.*)[)]\s*""".r
  val sumRE = """sum\s*[(](.*)[)]\s*""".r
  val litRE = """lit\s*[(](.*)[)]\s*""".r
}

sealed trait AttributeOp {
  val attributeName: String
  val opName: String
  def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute
}

trait AggregateOp extends AttributeOp {
  val aggregationName: String
}

object AttributeOp {
  import AggregationOpPatterns._
  def apply(attribute: String): AttributeOp = {
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
      case litRE(literal) =>
        LiteralOp(literal)
      case _ =>
        SelectOp(attribute.trim.toLowerCase)
    }
  }
}

case class CountOp(attributeName: String) extends AggregateOp {
  val aggregationName: String = "count"
  val opName: String = s"${aggregationName}(${attributeName})"
  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    val currGroup = context.currentGroup
    println(s"count Curr grp = ${currGroup}")
    eventInternal.getAttribute(attributeName) match {
      case Some(_) =>
        val count: Long = context.getAttribute(ContextGroup(currGroup, opName)).map(_.valueAs[Long]).getOrElse(0)
        context.addAttribute(ContextGroup(currGroup, opName), EventAttribute(opName, count+1))
      case None =>
        context.getAttribute(ContextGroup(currGroup, opName)).getOrElse(EventAttribute(opName, 0))
    }
  }
}

case class SumOp(attributeName: String) extends AggregateOp {
  val aggregationName: String = "sum"
  val opName: String = s"${aggregationName}(${attributeName})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    val currGroup = context.currentGroup
    eventInternal.getAttribute(attributeName) match {
      case Some(attr) =>
        val sum: Double = context.getAttribute(ContextGroup(currGroup, opName)).map(_.valueAs[Double]).getOrElse(0.0)
        context.addAttribute(ContextGroup(currGroup, opName), EventAttribute(opName, (sum+attr.valueAs[Double])))
      case None =>
        context.getAttribute(ContextGroup(currGroup, attributeName)).getOrElse(EventAttribute(opName, 0.0))
    }
  }
}

case class AvgOp(attributeName: String) extends AggregateOp {
  override val aggregationName: String = "avg"
  val opName: String = s"${aggregationName}(${attributeName})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    val currGroup = context.currentGroup
    eventInternal.getAttribute(attributeName) match {
      case Some(attr) =>
        val avg: Double = context.getAttribute(ContextGroup(currGroup, opName)).map(_.valueAs[Double]).getOrElse(0.0)
        val count: Int = context.getAttribute(ContextGroup(currGroup, s"count(${attributeName})")).map(_.valueAs[Int]).getOrElse(0)
        context.addAttribute(ContextGroup(currGroup, opName), EventAttribute(opName, ((avg*count)+attr.valueAs[Double]) / (count+1)))
        context.addAttribute(ContextGroup(currGroup, s"count(${attributeName})"), EventAttribute(s"count(${attributeName})", count+1))
      case None =>
        context.getAttribute(ContextGroup(currGroup, opName)).getOrElse(EventAttribute(opName, 0.0))
    }
  }
}

case class MaxOp(attributeName: String) extends AggregateOp {
  val aggregationName: String = "max"
  val opName: String = s"${aggregationName}(${attributeName})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    val currGroup = context.currentGroup
    val contextVal: Option[EventAttribute] = context.getAttribute(ContextGroup(currGroup, opName))
    val eventVal: Option[EventAttribute] = eventInternal.getAttribute(attributeName)

    contextVal match {
      case Some(cv) =>
        eventVal match {
          case Some(ev) if eventVal.compare(contextVal) > 0 =>
            context.addAttribute(ContextGroup(currGroup, opName), ev.as(opName))
          case _ =>
            context.addAttribute(ContextGroup(currGroup, opName), cv.as(opName))
        }
      case _ =>
        eventVal match {
          case Some(ev) =>
            context.addAttribute(ContextGroup(currGroup, opName), ev.as(opName))
          case _ =>
            context.addAttribute(ContextGroup(currGroup, opName), EventAttribute(opName, null))
        }
    }
  }
}

case class MinOp(attributeName: String) extends AggregateOp {
  val aggregationName: String = "min"
  val opName: String = s"${aggregationName}(${attributeName})"
  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    val currGroup = context.currentGroup
    val contextVal: Option[EventAttribute] = context.getAttribute(ContextGroup(currGroup, attributeName))
    val eventVal: Option[EventAttribute] = eventInternal.getAttribute(attributeName)

    contextVal match {
      case Some(cv) =>
         eventVal match {
           case Some(ev) if ev.compareTo(cv) < 0 =>
             context.addAttribute(ContextGroup(currGroup, opName), ev.as(opName))
           case _ =>
             context.addAttribute(ContextGroup(currGroup, opName), cv.as(opName))
         }
      case _ =>
        eventVal match {
          case Some(ev) =>
            context.addAttribute(ContextGroup(currGroup, opName), ev.as(opName))
          case _ =>
            context.addAttribute(ContextGroup(currGroup, opName), EventAttribute(opName, null))
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

trait LiteralOp extends AttributeOp

case class IntLiteralOp(value: Int) extends LiteralOp {
  override val attributeName: String = s"lit(${value})"
  override val opName: String = s"lit(${value})"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

case class LongLiteralOp(value: Long) extends LiteralOp {
  override val attributeName: String = s"lit-${value}"
  override val opName: String = s"lit-${value}"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

case class FloatLiteralOp(value: Float) extends LiteralOp {
  override val attributeName: String = s"lit-${value}"
  override val opName: String = s"lit-${value}"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

case class DoubleLiteralOp(value: Double) extends LiteralOp {
  override val attributeName: String = s"lit-${value}"
  override val opName: String = s"lit-${value}"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

case class BoolLiteralOp(value: Boolean) extends LiteralOp {
  override val attributeName: String = s"lit-${value}"
  override val opName: String = s"lit-${value}"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

case class StringLiteralOp(value: String) extends LiteralOp {
  override val attributeName: String = s"lit-${value}"
  override val opName: String = s"lit-${value}"

  override def op(eventInternal: EventInternal)(implicit context: ProcessingContext): EventAttribute = EventAttribute(attributeName, value)
}

object LiteralOp {
  def apply(value: Int): LiteralOp = {
    IntLiteralOp(value)
  }

  def apply(value: Long): LiteralOp = {
    LongLiteralOp(value)
  }

  def apply(value: Float): LiteralOp = {
    FloatLiteralOp(value)
  }

  def apply(value: Double): LiteralOp = {
    DoubleLiteralOp(value)
  }

  def apply(value: String): LiteralOp = {
    StringLiteralOp(value)
  }

  def apply(value: Boolean): LiteralOp = {
    BoolLiteralOp(value)
  }
}

trait AttributeResolver {
  def resolve(attributeOp: AttributeOp, event: EventInternal)(implicit context: ProcessingContext): EventAttribute = {
    attributeOp match {
      case lit: LiteralOp =>
        lit.op(event)(context)
      case SelectOp(_) =>
        event.getAttribute(attributeOp.opName).getOrElse(EventAttribute(attributeOp.opName, null))
      case aggOp: AggregateOp =>
        context.getAttribute(ContextGroup(context.currentGroup, aggOp.opName)).getOrElse(EventAttribute(aggOp.opName, null))
    }
  }
}