package com.fndef.streams.core.function

import com.fndef.streams.PipelineSpec
import com.fndef.streams.core.{EventBatch, EventInternal, StreamFunction, StreamPacket}

import scala.util.{Failure, Success, Try}

class SelectFunction(val name: String, pipelineSpec: PipelineSpec) extends StreamFunction {

  override def process(packet: StreamPacket): Unit = {
    Try {
      packet match {
        case EventBatch(batchTime, events) =>
          println(s"Start selecting :: number of events in batch - ${events.size}")
          val select = pipelineSpec.selections.map(_.opName)
          val selectEvents = events.map(e => EventInternal(e.eventType, e.eventTime, pipelineSpec.literals ++ e.attributes.filter(a => select.contains("*") || select.contains(a.name))))
          val batch = EventBatch(batchTime, selectEvents)
          println(s"selected event = ${batch}")
          sinks.foreach(_.process(batch))
        case _ =>
          // process other events
      }
    } match {
      case Success(_) =>
        println("select complete without error")
      case Failure(exception) =>
        println("error in select ")
        exception.printStackTrace()
    }
  }
}