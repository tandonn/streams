package com.fndef.streams.core.function

import com.fndef.streams.core.common.FilterOp
import com.fndef.streams.core.{EventInternal, ProcessingContext, StreamFunction}

class FilterFunction(val name: String, filters: Seq[FilterOp]) extends StreamFunction {

  override def processInternal(event: EventInternal)(implicit context: ProcessingContext): Option[EventInternal] = {
    val accept = filters.map(_.op(event)).foldLeft(true)((a, v) => a && v)
    if (accept) {
      Option(event)
    } else {
      None
    }

  }
}