package com.fndef.streams.core.function

import com.fndef.streams.core.common.AttributeOp
import com.fndef.streams.core.{EventInternal, ProcessingContext, StreamFunction}

class GroupFunction(val name: String, groupings: Seq[AttributeOp], aggregations: Seq[AttributeOp]) extends StreamFunction {
  override def processInternal(event: EventInternal)(implicit context: ProcessingContext): Option[EventInternal] = {
    context.currentGroup = groupings.map(g => g.opName).map(n => event.getAttribute(n).map(_.value).map(_.toString).getOrElse(""))
    Option(EventInternal(event.eventType, event.attributes ++ aggregations.map(a => a.op(event))))
  }
}
