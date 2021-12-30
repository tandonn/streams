package com.fndef.streams.core.function

import com.fndef.streams
import com.fndef.streams.core.{EventInternal, ProcessingContext, StreamFunction, StreamSink}

class ToFunction(val name: String, sinks: Seq[StreamSink]) extends StreamFunction {
  streams.registerSinks(this)(sinks:_*)

  override def processInternal(event: EventInternal)(implicit context: ProcessingContext): Option[EventInternal] = {
    println(s"to event = ${event}")
    Option(event)
  }

}

object ToFunction {
  def apply(name: String, sinks: StreamSink*): ToFunction = {
    new ToFunction(name, sinks)
  }
}
