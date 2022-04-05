package com.fndef.streams.core

import scala.collection.mutable

trait Source[+T] {
  def registerSink[U <: IdentifiableSink[T]](sink: U): Unit
  def deregisterSink[U <: IdentifiableSink[T]](sink: U): Unit
}

trait IdentifiableSource[+T] extends Source[T] with Identifiable {
  def registerSink[U <: IdentifiableSink[T]](sink: U): Unit
  def deregisterSink[U <: IdentifiableSink[T]](sink: U): Unit
}

trait PipelineSource extends IdentifiableSource[EventInternal] with PipelineSinkRegistry {
  def registerSink[U <: IdentifiableSink[EventInternal]](pipelineSink: U): Unit
  def deregisterSink[U <: IdentifiableSink[EventInternal]](pipelineSink: U): Unit
}

trait StreamSource extends IdentifiableSource[StreamPacket] with StreamSinkRegistry {
  def registerSink[U <: IdentifiableSink[StreamPacket]](streamSink: U): Unit
  def deregisterSink[U <: IdentifiableSink[StreamPacket]](streamSink: U): Unit
}

trait SinkRegistry[T] {
  private val interestedSinks: mutable.Set[IdentifiableSink[T]] = mutable.Set()
  def sinks: Seq[IdentifiableSink[T]] = interestedSinks.synchronized(interestedSinks.toSeq)

  def registerSink[U <: IdentifiableSink[T]](streamSink: U): Unit = {
    interestedSinks.synchronized {
      assert(streamSink != null, "Null sink")
      interestedSinks += streamSink
      println(s"registered sink ${streamSink.id}")
    }
  }

  def deregisterSink[U <: IdentifiableSink[T]](streamSink: U): Unit = {
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
