package com.fndef.streams.core

import java.util.UUID

trait StreamFunction extends StreamSource with StreamSink {
  val id: UUID = UUID.randomUUID()
  val name: String

  override def processEvent(event: EventInternal)(implicit context: ProcessingContext): EventInternal = {
    Option(event) match {
      case Some(e) =>
        val outcome: Option[EventInternal] = processInternal(e)
        outcome match {
          case Some(e) =>
            notify(e)
          case None =>
        }
        outcome.getOrElse(null)
      case None =>
        null
    }
  }

  def notify(eventInternal: EventInternal)(implicit context: ProcessingContext): EventInternal = {
    sinks.foreach(_.processEvent(eventInternal))
    eventInternal
  }

  def processInternal(event: EventInternal)(implicit context: ProcessingContext): Option[EventInternal] = Option(event)
}

