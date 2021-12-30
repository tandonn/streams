package com.fndef.streams.core.common

import com.fndef.streams.core.EventInternal

sealed trait WinState
case object Ready extends WinState
case object Expired extends WinState
case object Error extends WinState

trait WindowOp {
  def windowOp(event: EventInternal): EventInternal
}

trait WindowType {
  def winState: WinState
}

class ContinousWindow extends WindowType {
  override def winState: WinState = Ready
}

case class WindowSpec(pipelineName: String, literals: Seq[AttributeOp] = Seq(),
                      groupings: Seq[AttributeOp] = Seq(),
                      aggregations: Seq[AttributeOp] = Seq(),
                      filters: Seq[FilterOp] = Seq(),
                      projections: Seq[AttributeOp] = Seq(AttributeOp("*")))
