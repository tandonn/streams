package com.fndef.streams.core.store

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.fndef.streams.WindowSpec
import com.fndef.streams.core.{EventInternal, Startable, StreamSink}

class IndexedStreamStore(val id: String, windowSpec: WindowSpec, pipeline: StreamSink) extends StreamStore with Startable {
  private[this] val inputQueue: BlockingQueue[EventInternal] = new LinkedBlockingQueue[EventInternal]()
  private[this] val streamTask = new StreamStoreTask(inputQueue, new IndexedEventStream(EventTimeResolver(windowSpec)), new StreamFrames(windowSpec),pipeline, windowSpec)
  private[this] val storeRunner = new StreamStoreRunner(s"${id}-store-runner", windowSpec, streamTask)

  override def addEvent(event: EventInternal): Boolean = {
    if (storeRunner.isActive) {
      println(s"adding event to pending queue = ${event}")
      inputQueue.put(event)
    }
    println(s"pending queue size now = ${inputQueue.size()}")
    storeRunner.isActive
  }

  override def startup: Boolean = {
    storeRunner.startup
    println(s"Stream store [${id}] started - ${isActive}")
    isActive
  }

  override def shutdown: Boolean = {
    println(s"shutting down stream store - ${id}")
    storeRunner.shutdown
    println(s"stream store - ${id} is now down - ${!isActive}")
    !isActive
  }

  override def isActive: Boolean = storeRunner.isActive
}
