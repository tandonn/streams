package com.fndef.streams.core.function

import com.fndef.streams.core.common.AttributeOp
import com.fndef.streams.core.{EventInternal, ProcessingContext, StreamFunction}

class SelectFunction(val name: String, projections: Seq[AttributeOp]) extends StreamFunction {

  override def processInternal(event: EventInternal)(implicit context: ProcessingContext): Option[EventInternal] = {
    val selectAttributes = projections.map(_.opName)
    Option(EventInternal(event.eventType, event.attributes.filter(a => selectAttributes.contains("*") || selectAttributes.contains(a.name))))
  }
}