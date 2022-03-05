package com.fndef.streams.core.store

import java.time.LocalDateTime
import java.util.concurrent.locks.ReentrantLock

import com.fndef.streams.WindowSpec
import com.fndef.streams.core.EventInternal

trait EventStream {
  def insert(event: EventInternal): (LocalDateTime, EventInternal)
  def size: Int
  def slideTo(startTime: LocalDateTime): EventStream
  def slice(startTime: LocalDateTime, endTime: LocalDateTime): Seq[(LocalDateTime, EventInternal)]
}

trait WindowedEventStream extends EventStream {
  def dequeueDirtyFrames: Seq[Frame]
}

class IndexedEventStream(windowSpec: WindowSpec) extends WindowedEventStream {
  private[this] val lock = new ReentrantLock()
  private[this] val eventTimeResolver: EventTimeResolver = EventTimeResolver(windowSpec)
  private[this] var eventStream: Seq[(LocalDateTime, EventInternal)] = Vector()
  private[this] val frames = new StreamFrames(windowSpec)

  def insert(event: EventInternal): (LocalDateTime, EventInternal) = {
    val eventEntry: (LocalDateTime, EventInternal) = (eventTimeResolver.eventTime(event), event)
    try {
      lock.lockInterruptibly()
      eventStream = if (eventStream.isEmpty) {
        eventEntry +: eventStream
      } else if (eventEntry._1.isEqual(eventStream.last._1) || eventEntry._1.isAfter(eventStream.last._1)) {
        eventStream :+ eventEntry
      } else {
        val idx = eventStream.lastIndexWhere(e => eventEntry._1.isEqual(e._1) || eventEntry._1.isAfter(e._1))
        if (idx > -1) {
          val slices = eventStream.splitAt(idx+1)
          (slices._1 :+ eventEntry) ++ slices._2
        } else {
          eventEntry +: eventStream
        }
      }
      frames.createPendingFrames(eventEntry._1)
      frames.markFrameDirty(eventEntry._1)
      eventEntry
    } finally {
      lock.unlock()
    }
  }

  def size: Int = {
    try {
      lock.lockInterruptibly()
      eventStream.length
    } finally {
      lock.unlock()
    }
  }

  def slideTo(startTime: LocalDateTime): EventStream = {
    println(s" no of events before sliding ${eventStream.size}")
    try {
      lock.lockInterruptibly()
      val idx = eventStream.indexWhere(e => e._1.isAfter(startTime))
      if (idx >= 0) {
        eventStream = eventStream.slice(idx, eventStream.size)
      } else {
        eventStream = Vector()
      }

      frames.createPendingFrames(LocalDateTime.now())
      frames.removeExpiredFrames(startTime)
      println(s" no of events after sliding ${eventStream.size}")
    } finally {
      lock.unlock()
    }
    this
  }

  def slice(startTime: LocalDateTime, endTime: LocalDateTime): Seq[(LocalDateTime, EventInternal)] = {
    println(s"current stream now :: ${eventStream.map(_._2)}")
    println(s"current stream with time :: ${eventStream}")
    try {
      lock.lockInterruptibly()
      eventStream.filter(e => e._1.isAfter(startTime) && e._1.isBefore(endTime))
    } finally {
      lock.unlock()
    }
  }

  override def dequeueDirtyFrames: Seq[Frame] = {
    try {
      lock.lockInterruptibly()
      frames.markFramesComplete(LocalDateTime.now())
      frames.collectDirtyFrames
    } finally {
      lock.unlock()
    }
  }
}