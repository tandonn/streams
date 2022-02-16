package com.fndef.streams.core.function

import com.fndef.streams.PipelineSpec
import com.fndef.streams.core.operation.AttributeOp
import com.fndef.streams.core.{EventAttribute, EventBatch, EventInternal, ProcessingContext, StreamFunction, StreamPacket}

class GroupFunction(val name: String, pipelineSpec: PipelineSpec) extends StreamFunction {
  private[this] val aggregations: Seq[AttributeOp] = pipelineSpec.aggregations
  private[this] val eventTimeAttrName: String = pipelineSpec.windowSpec.timeAttribute

  override def process(packet: StreamPacket): Unit = {
    def aggregateEvents(events: Seq[EventInternal])(implicit pc : ProcessingContext): ProcessingContext = {
      events match {
        case head +: tail =>
          registerAttribute(head, eventTimeAttrName, pc)
          aggregations.map(_.op(head))
          aggregateEvents(tail)
        case Seq() =>
          pc
      }
    }

    packet match {
      case EventBatch(batchTime, events) =>
        val aggregatedBatch = pipelineSpec.grouping match {
          case _ +: _ =>
            val processingContext = aggregateEvents(events)(ProcessingContext(pipelineSpec))
            val aggregatedEvents = processingContext.groupingContext.listContext.map(strictGroupEvent  orElse allAttributesEvent)
            EventBatch(batchTime, aggregatedEvents)
          case _ =>
            EventBatch(batchTime, events)
        }
        println(s"aggregated batch = $aggregatedBatch")
        sinks.foreach(_.process(aggregatedBatch))
      case _ =>
        // other events
    }
  }

  private def registerAttribute(event: EventInternal, attrName: String, pc: ProcessingContext): Unit = {
    event.getAttribute(attrName).foreach(a => pc.groupingContext.addAttribute(event, a))
  }

  private def strictGroupEvent: PartialFunction[(Seq[EventAttribute], EventInternal, Seq[EventAttribute]), EventInternal] = {
    case entry if pipelineSpec.applyStrictGrouping => EventInternal(entry._2.eventType, entry._2.eventTime, entry._1 ++ entry._3)
  }

  private def allAttributesEvent: PartialFunction[(Seq[EventAttribute], EventInternal, Seq[EventAttribute]), EventInternal] = {
    case entry if ! pipelineSpec.applyStrictGrouping => EventInternal(entry._2.eventType, entry._2.eventTime, entry._2.attributes ++ entry._3)
  }
}