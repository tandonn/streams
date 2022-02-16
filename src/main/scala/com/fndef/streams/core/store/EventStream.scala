package com.fndef.streams.core.store

import java.time.LocalDateTime

import com.fndef.streams.core.EventInternal

trait EventStream {
  def insert(event: EventInternal): (LocalDateTime, EventInternal)
  def size: Int
  def slideTo(startTime: LocalDateTime): EventStream
  def slice(startTime: LocalDateTime, endTime: LocalDateTime): Seq[(LocalDateTime, EventInternal)]
}

class IndexedEventStream(eventTimeResolver: EventTimeResolver) extends EventStream {
  private[this] var eventStream: Seq[(LocalDateTime, EventInternal)] = Vector()

  def insert(event: EventInternal): (LocalDateTime, EventInternal) = {
    val eventEntry: (LocalDateTime, EventInternal) = (eventTimeResolver.eventTime(event), event)
    val idx = eventStream.indexWhere(e => eventEntry._1.isEqual(e._1) || eventEntry._1.isAfter(e._1) )
    eventStream = if (idx > 0) {
      (eventStream.slice(0, idx) :+ eventEntry) ++ eventStream.slice(idx, eventStream.length)
    } else if (idx == 0) {
      eventEntry +: eventStream
    } else {
      eventStream :+ eventEntry
    }
    eventEntry
  }

  def size: Int = {
    eventStream.length
  }

  def slideTo(startTime: LocalDateTime): EventStream = {
    println(s" no of events before sliding ${eventStream.size}")
    val idx = eventStream.lastIndexWhere(e => e._1.isAfter(startTime))
    if (idx >= 0) {
      eventStream = eventStream.slice(0, idx+1)
    } else {
      eventStream = Vector()
    }
    println(s" no of events after sliding ${eventStream.size}")
    this
  }

  def slice(startTime: LocalDateTime, endTime: LocalDateTime): Seq[(LocalDateTime, EventInternal)] = {
    println(s"current stream now :: ${eventStream.map(_._2)}")
    println(s"current stream with time :: ${eventStream}")
    eventStream.filter(e => e._1.isAfter(startTime) && e._1.isBefore(endTime))
  }
}