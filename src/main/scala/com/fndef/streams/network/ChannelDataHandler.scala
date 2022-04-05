package com.fndef.streams.network

import java.nio.channels.Selector
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

abstract class ChannelDataHandler(id: String) extends ServerThread(id) {
  def registerChannel(endpointTarget: EndpointTarget): EndpointTarget
  def startInternal: Boolean
  def stopInternal: Boolean
  def handlerLoop: Unit

  private[this] val latch = new CountDownLatch(1)
  private[this] val lock = new ReentrantLock()
  private[this] val active: AtomicBoolean = new AtomicBoolean(false)
  private[this] val stopped: AtomicBoolean = new AtomicBoolean(false)
  lazy private[this] val selector: Selector = Selector.open()

  override def startup: Boolean = {
    try {
      lock.lockInterruptibly()
      require(!stopped.get(), s"server write handler [${id}] is stopped")
      require(!active.get(), s"server write handler [${id}] is already started")
      startInternal
      latch.await()
      isActive
    } finally {
      lock.unlock()
    }
  }

  override def shutdown: Boolean = {
    try {
      lock.lockInterruptibly()
      active.set(false)
      stopped.set(true)
      stopInternal
      stopped.get()
    } finally {
      lock.unlock()
    }
  }

  override def isActive: Boolean = {
    try {
      lock.lockInterruptibly()
      active.get() && !stopped.get()
    } finally {
      lock.unlock()
    }
  }

  override def run(): Unit = {
    println(s"handler [${id}] initialized")
    try {
      active.set(true)
      latch.countDown()
      handlerLoop
    } finally {
      println(s"handler [${id}] is exiting")
      active.set(false)
      stopped.set(true)
    }
  }
}
