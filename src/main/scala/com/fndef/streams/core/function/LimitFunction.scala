package com.fndef.streams.core.function

import com.fndef.streams.PipelineSpec
import com.fndef.streams.core.{EventBatch, StreamFunction, StreamPacket}

class LimitFunction(val name: String, pipelineSpec: PipelineSpec) extends StreamFunction {
  override def process(packet: StreamPacket): Unit = {
    packet match {
      case EventBatch(batchTime, events) =>
        val batch = if (pipelineSpec.fetchCount > -1) {
          EventBatch(batchTime, events.take(pipelineSpec.fetchCount))
        } else {
          EventBatch(batchTime, events)
        }
        sinks.foreach(_.process(batch))
      case _ =>
      // other events
    }
  }
}
