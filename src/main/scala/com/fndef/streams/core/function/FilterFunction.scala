package com.fndef.streams.core.function

import com.fndef.streams.core.operation.FilterOp
import com.fndef.streams.core.{EventBatch, StreamFunction, StreamPacket}

class FilterFunction(val name: String, filters: Seq[FilterOp]) extends StreamFunction {

  override def process(packet: StreamPacket): Unit = {
    println(s"Start filtering")
    packet match {
      case b: EventBatch =>
        println(s"events in packet :: ${b.events.size} ")
        println(s"events to filter ${b.events}")
        val filtered = b.events.filter(e => filters.map(_.op(e)).foldLeft(true)((a, v) => a && v))
        val filteredBatch: StreamPacket = EventBatch(b.batchTime, filtered)
        println(s"filtered batch = ${filteredBatch}")
        sinks.foreach(_.process(filteredBatch))
      case _ =>
        // other events process here
    }
  }
}