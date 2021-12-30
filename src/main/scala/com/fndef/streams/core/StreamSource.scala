package com.fndef.streams.core

import scala.collection.mutable

trait StreamSource {
  private val interestedSinks: mutable.Set[StreamSink] = mutable.Set()
  def sinks: Seq[StreamSink] = interestedSinks.toSeq

  def registerSink(streamSink: StreamSink): StreamSource = {
    interestedSinks.synchronized {
      assert(streamSink != null, "Null sink")
      interestedSinks += streamSink
    }
    this
  }

  def deregisterSink(streamSink: StreamSink): StreamSource = {
    interestedSinks.synchronized {
      interestedSinks -= streamSink
    }
    this
  }

  def clearSinks: StreamSource = {
    interestedSinks.clear()
    this
  }
}