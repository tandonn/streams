package com.fndef.streams.core.function

import com.fndef.streams.core.{EventBatch, EventInternal, IdentifiableSink, StreamFunction, StreamPacket}

class ToFunction(val name: String, pipeline: Pipeline) extends StreamFunction {
  override def process(data: StreamPacket): Unit = {
    def publishEvents(events: Seq[EventInternal], sink: IdentifiableSink[EventInternal]): Unit = {
      events.foreach(sink.process(_))
    }

    data match {
      case EventBatch(_, events) =>
        pipeline.sinks.foreach(publishEvents(events, _))
      case _ =>
        // other type of events
    }
  }
}