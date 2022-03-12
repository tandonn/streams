package com.fndef.streams.core.function

import com.fndef.streams.PipelineSpec
import com.fndef.streams.core.operation.{SortOp, SortOrder}
import com.fndef.streams.core.{EventAttribute, EventBatch, EventInternal, StreamFunction, StreamPacket}

class SortFunction(val name: String, pipelineSpec: PipelineSpec) extends StreamFunction {
  override def process(packet: StreamPacket): Unit = {

    packet match {
      case EventBatch(batchTime, events) =>
        val sorted = pipelineSpec.sorting match {
          case _ +: _ =>
            println(s"sort spec : ${pipelineSpec.sorting}")
            println(s"sorted batch :: ${sortBatch(events)}")
            EventBatch(batchTime, sortBatch(events))
          case _ =>
            EventBatch(batchTime, events)
        }
        sinks.foreach(_.process(sorted))
      case _ =>
        // other event types
    }
  }

  def sortBatch(events: Seq[EventInternal]): Seq[EventInternal] = {
    events.sortWith((a, b) => sortFn(a, b) < 0 )
  }

  private[this] def sortFn(e1: EventInternal, e2: EventInternal): Int = {
    def sort(e1: EventInternal, e2: EventInternal, sortSpec: Seq[SortOp], order: Int): Int = {
      sortSpec match {
        case currSort +: pendingSpec =>
          val a1 = e1.getAttribute(currSort.attributeOp.attributeName)
          val a2 = e2.getAttribute(currSort.attributeOp.attributeName)
          val compareOrder = compareAttr(a1, a2, currSort.sortOrder)
          if (compareOrder != 0) {
            compareOrder
          } else {
            sort(e1, e2, pendingSpec, 0)
          }
        case Seq() =>
          order
      }
    }

    sort(e1, e2, pipelineSpec.sorting, 0)
  }

  private[this] def compareAttr(a1: Option[EventAttribute], a2: Option[EventAttribute], sortOrder: SortOrder): Int = {
    a1 match {
      case Some(a) =>
        a2 match {
          case Some(b) =>
            a.compare(b) * sortOrder.order
          case None =>
            1 * sortOrder.order
        }
      case None =>
        a2 match {
          case Some(_) =>
            -1 * sortOrder.order
          case None =>
            0
        }
    }
  }
}
