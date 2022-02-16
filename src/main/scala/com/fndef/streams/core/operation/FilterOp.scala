package com.fndef.streams.core.operation

import com.fndef.streams.core.{EventAttribute, EventInternal}

sealed trait FilterOp {
  def op(event: EventInternal): Boolean
  def opName: String
}

trait FilterAttributeResolver {
  def attributeResolver: EventInternal => Option[EventAttribute]
  def attributeName: String
}

class EventAttributeResolver(attrName: String) extends FilterAttributeResolver {
  override def attributeResolver: EventInternal => Option[EventAttribute] = (event: EventInternal) => {
    event.getAttribute(attrName)
  }

  override def attributeName: String = attrName
}

class LiteralResolver(lit: EventAttribute) extends FilterAttributeResolver {
  override def attributeResolver: EventInternal => Option[EventAttribute] = (event: EventInternal) => {
    Option(lit)
  }

  override def attributeName: String = lit.name
}

case class NoFilter(attrName: String) extends FilterOp {
  override def op(event: EventInternal): Boolean = true

  override def opName: String = s"NoOp(${attrName})"
}

case class NotNullFilter(resolver: FilterAttributeResolver) extends FilterOp {
  override def op(event: EventInternal): Boolean = {
    resolver.attributeResolver(event) match {
      case Some(attr) =>
        Option(attr.value).nonEmpty
      case None =>
        false
    }
  }

  override def opName: String = s"${resolver.attributeName} is not null"
}

case class IsNullFilter(resolver: FilterAttributeResolver) extends FilterOp {
  override def op(event: EventInternal): Boolean = {
    resolver.attributeResolver(event) match {
      case Some(attr) =>
        Option(attr.value).isEmpty
      case None =>
        true
    }
  }

  override def opName: String = s"${resolver.attributeName} is null"
}

case class EqualFilter(left: FilterAttributeResolver, right: FilterAttributeResolver) extends ComparisionFilter(left, right) {
  override def op(event: EventInternal): Boolean = {
    compareAttributes(event) == 0
  }

  override def opName: String = s"${left.attributeName} == ${right.attributeName}"
}

case class NotEqualFilter(left: FilterAttributeResolver, right: FilterAttributeResolver) extends ComparisionFilter(left, right) {
  override def op(event: EventInternal): Boolean = {
    compareAttributes(event) != 0
  }

  override def opName: String = s"${left.attributeName} == ${right.attributeName}"
}

case class NotFilter(filterOp: FilterOp) extends FilterOp {
  override def op(event: EventInternal): Boolean = {
    ! filterOp.op(event)
  }

  override def opName: String = s"not(${filterOp.opName})"
}

case class AndFilter(left: FilterOp, right: FilterOp) extends FilterOp {
  override def op(event: EventInternal): Boolean = {
    left.op(event) && right.op(event)
  }

  override def opName: String = s"(${left.opName} and ${right.opName})"
}

case class OrFilter(left: FilterOp, right: FilterOp) extends FilterOp {
  override def op(event: EventInternal): Boolean = {
    left.op(event) || right.op(event)
  }

  override def opName: String = s"(${left.opName} or ${right.opName})"
}

abstract class ComparisionFilter(left: FilterAttributeResolver, right: FilterAttributeResolver) extends FilterOp {
  def compareAttributes(event: EventInternal): Int = {
    val leftAttr = left.attributeResolver(event)
    val rightAttr = right.attributeResolver(event)
    leftAttr match {
      case Some(l) =>
        rightAttr match {
          case Some(r) =>
            l.compare(r)
          case None =>
            1
        }
      case None =>
        rightAttr match {
          case Some(r) =>
            -1
          case None =>
            0
        }
    }
  }
}
case class GreaterThan(left: FilterAttributeResolver, right: FilterAttributeResolver) extends ComparisionFilter(left, right) {
  override def op(event: EventInternal): Boolean = {
    compareAttributes(event) > 0
  }

  override def opName: String = s"(${left.attributeName} > ${right.attributeName})"
}

case class GreaterThanEqual(left: FilterAttributeResolver, right: FilterAttributeResolver) extends ComparisionFilter(left, right) {
  override def op(event: EventInternal): Boolean = {
    compareAttributes(event) >= 0
  }

  override def opName: String = s"(${left.attributeName} >= ${right.attributeName})"
}

case class LessThan(left: FilterAttributeResolver, right: FilterAttributeResolver) extends ComparisionFilter(left, right) {
  override def op(event: EventInternal): Boolean = {
    compareAttributes(event) < 0
  }

  override def opName: String = s"(${left.attributeName} < ${right.attributeName})"
}

case class LessThanEqual(left: FilterAttributeResolver, right: FilterAttributeResolver) extends ComparisionFilter(left, right) {
  override def op(event: EventInternal): Boolean = {
    compareAttributes(event) <= 0
  }

  override def opName: String = s"(${left.attributeName} <= ${right.attributeName})"
}
