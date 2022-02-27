package com.fndef.streams.core

import scala.collection.mutable

trait Source[+T] {
  def registerSink[U <: Sink[T]](sink: U): Unit
  def deregisterSink[U <: Sink[T]](sink: U): Unit
}

trait PipelineSource extends Source[EventInternal] with PipelineSinkRegistry {
  def registerSink[U <: Sink[EventInternal]](pipelineSink: U): Unit
  def deregisterSink[U <: Sink[EventInternal]](pipelineSink: U): Unit
}

trait StreamSource extends Source[StreamPacket] with StreamSinkRegistry {
  def registerSink[U <: Sink[StreamPacket]](streamSink: U): Unit
  def deregisterSink[U <: Sink[StreamPacket]](streamSink: U): Unit
}

trait SinkRegistry[T] {
  private val interestedSinks: mutable.Set[Sink[T]] = mutable.Set()
  def sinks: Seq[Sink[T]] = interestedSinks.synchronized(interestedSinks.toSeq)

  def registerSink[U <: Sink[T]](streamSink: U): Unit = {
    interestedSinks.synchronized {
      assert(streamSink != null, "Null sink")
      interestedSinks += streamSink
    }
  }

  def deregisterSink[U <: Sink[T]](streamSink: U): Unit = {
    interestedSinks.synchronized {
      interestedSinks -= streamSink
    }
  }

  def clearSinks: Unit = {
    interestedSinks.clear()
  }
}

trait StreamSinkRegistry extends SinkRegistry[StreamPacket]
trait PipelineSinkRegistry extends SinkRegistry[EventInternal]
