package com.fndef.streams.core.function

import com.fndef.streams.core.common.AttributeOp
import com.fndef.streams.core.{EventAttribute, EventInternal, ProcessingContext, StreamFunction}

class EnrichLiteral(val name: String, literals: Seq[AttributeOp]) extends StreamFunction {
  override def processInternal(event: EventInternal)(implicit context: ProcessingContext): Option[EventInternal] = {
    val literalAttrs: Seq[EventAttribute] = literals.map(_.op(event))
    Option(EventInternal(event.eventType, event.attributes ++ literalAttrs))
  }
}
