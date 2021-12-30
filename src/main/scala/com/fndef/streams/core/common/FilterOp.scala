package com.fndef.streams.core.common

import com.fndef.streams.core.{ContextGroup, EventInternal, ProcessingContext}

sealed trait FilterOp {
  def op(event: EventInternal)(implicit context: ProcessingContext): Boolean
  def opName: String
}

case class NotNullFilter(attribute: AttributeOp) extends FilterOp {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    attribute match {
      case lit: LiteralOp =>
        event.getAttribute(lit.attributeName).nonEmpty
      case attr: SelectOp =>
        event.getAttribute(attr.attributeName).nonEmpty
      case attr: AggregateOp =>
        val opName = attr.opName
        context.getAttribute(ContextGroup(context.currentGroup, opName)).nonEmpty
    }
  }

  override def opName: String = s"${attribute.attributeName} is not null"
}

object NotNullFilter {
  def apply(attribute: String): NotNullFilter = new NotNullFilter(AttributeOp(attribute))
}

case class IsNullFilter(attribute: AttributeOp) extends FilterOp {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    attribute match {
      case lit: LiteralOp =>
        event.getAttribute(lit.attributeName).isEmpty
      case attr: SelectOp =>
        event.getAttribute(attr.attributeName).isEmpty
      case attr: AggregateOp =>
        val opName = attr.opName
        context.getAttribute(ContextGroup(context.currentGroup, opName)).isEmpty
    }
  }

  override def opName: String = s"${attribute.attributeName} is null"
}

object IsNullFilter {
  def apply(attribute: String): IsNullFilter = new IsNullFilter(AttributeOp(attribute))
}

case class EqualFilter(left: AttributeOp, right: AttributeOp) extends FilterOp with AttributeResolver {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    val leftAttr = resolve(left, event)
    val rightAttr = resolve(right, event)
    println(s"equalop $leftAttr --- $rightAttr")
    (leftAttr == rightAttr) || (leftAttr.compare(rightAttr) == 0)
  }

  override def opName: String = s"${left.opName} == ${right.opName}"
}

object EqualFilter {
  def apply(left: String, right: String): EqualFilter = new EqualFilter(AttributeOp(left), AttributeOp(right))
}

case class NotEqualFilter(left: AttributeOp, right: AttributeOp) extends FilterOp with AttributeResolver {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    val leftAttr = resolve(left, event)
    val rightAttr = resolve(right, event)

    leftAttr != rightAttr && leftAttr.compare(rightAttr) != 0
  }

  override def opName: String = s"${left.opName} == ${right.opName}"
}

object NotEqualFilter {
  def apply(left: String, right: String): NotEqualFilter = new NotEqualFilter(AttributeOp(left), AttributeOp(right))
}

case class NotFilter(filterOp: FilterOp) extends FilterOp {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    ! filterOp.op(event)
  }

  override def opName: String = s"not(${filterOp.opName})"
}

case class AndFilter(left: FilterOp, right: FilterOp) extends FilterOp {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    left.op(event) && right.op(event)
  }

  override def opName: String = s"(${left.opName} and ${right.opName})"
}

case class OrFilter(left: FilterOp, right: FilterOp) extends FilterOp {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    left.op(event) || right.op(event)
  }

  override def opName: String = s"(${left.opName} or ${right.opName})"
}

case class GreaterThan(left: AttributeOp, right: AttributeOp) extends FilterOp with AttributeResolver {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    resolve(left, event).compare(resolve(right, event)) > 0
  }

  override def opName: String = s"(${left.opName} > ${right.opName})"
}

object GreaterThan {
  def apply(left: String, right: String): GreaterThan = new GreaterThan(AttributeOp(left), AttributeOp(right))
}

case class GreaterThanEqual(left: AttributeOp, right: AttributeOp) extends FilterOp with AttributeResolver {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    resolve(left, event).compare(resolve(right, event)) >= 0
  }

  override def opName: String = s"(${left.opName} >= ${right.opName})"
}

object GreaterThanEqual {
  def apply(left: String, right: String): GreaterThanEqual = new GreaterThanEqual(AttributeOp(left), AttributeOp(right))
}

case class LessThan(left: AttributeOp, right: AttributeOp) extends FilterOp with AttributeResolver {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    resolve(left, event).compare(resolve(right, event)) < 0
  }

  override def opName: String = s"(${left.opName} < ${right.opName})"
}

object LessThan {
  def apply(left: String, right: String): LessThan = new LessThan(AttributeOp(left), AttributeOp(right))
}

case class LessThanEqual(left: AttributeOp, right: AttributeOp) extends FilterOp with AttributeResolver {
  override def op(event: EventInternal)(implicit context: ProcessingContext): Boolean = {
    resolve(left, event).compare(resolve(right, event)) <= 0
  }

  override def opName: String = s"(${left.opName} <= ${right.opName})"
}

object LessThanEqual {
  def apply(left: String, right: String): LessThanEqual = new LessThanEqual(AttributeOp(left), AttributeOp(right))
}