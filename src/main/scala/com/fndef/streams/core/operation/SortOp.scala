package com.fndef.streams.core.operation

trait SortOrder {
  def order: Int
}
case object SortAsc extends SortOrder {
  override def order: Int = 1
}

case object SortDesc extends SortOrder {
  override def order: Int = -1
}

case class SortOp(attributeOp: AttributeOp, sortOrder: SortOrder = SortAsc)

