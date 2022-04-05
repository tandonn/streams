package com.fndef.streams.core

sealed abstract class ResponseStatus(val status: Byte)
case object Success extends ResponseStatus(0)
case object Failed extends ResponseStatus(1)
case object Ignored extends ResponseStatus(2)
case object Queued extends ResponseStatus(3)
case object Running extends ResponseStatus(4)
case object ExecutionPaused extends ResponseStatus(5)
case object DuplicateIgnored extends ResponseStatus(6)
case object DuplicateRetried extends ResponseStatus(7)
case object TooManyTries extends ResponseStatus(8)

object ResponseStatus {
  def apply(status: Int): ResponseStatus = {
    status match {
      case Success.status =>
        Success
      case Failed.status =>
        Failed
      case Ignored.status =>
        Ignored
      case Queued.status =>
        Queued
      case Running.status =>
        Running
      case ExecutionPaused.status =>
        ExecutionPaused
      case DuplicateIgnored.status =>
        DuplicateIgnored
      case DuplicateRetried.status =>
        DuplicateRetried
      case TooManyTries.status =>
        TooManyTries
      case _ =>
        println(s"Invalid status code - ${status}")
        throw new IllegalArgumentException(s"Invalid status code - ${status}")
    }
  }
}

abstract class Response(val correlationId: String, val status: ResponseStatus)
