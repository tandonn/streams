package com.fndef.streams.network

import java.nio.channels.SocketChannel
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

case class EndpointTarget(endpoint: ChannelEndpoint, target: EndpointTargetType)

trait EndpointTargetType
case object ReadEndpoint extends EndpointTargetType
case object WriteEndpoint extends EndpointTargetType

abstract class ChannelEndpoint(val id: String, val channel: SocketChannel) {
  private[this] val remoteAddrStr = channel.getRemoteAddress.toString
  private[this] val lock = new ReentrantLock()
  private[this] val closed = new AtomicBoolean(false)

  override def toString: String = {
    s"ChannelEndpoint{id=${id}, remoteStr=${remoteAddrStr}}"
  }

  override def hashCode(): Int = (id.hashCode + channel.hashCode)

  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[ChannelEndpoint]) {
      val other = obj.asInstanceOf[ChannelEndpoint]
      id == other.id && channel.equals(other.channel)
    } else false
  }

  def isOpen: Boolean = {
    try {
      lock.lockInterruptibly()
      !closed.get() && channel.isConnected
    } finally {
      lock.unlock()
    }
  }

  def close: Unit = {
    try {
      lock.lockInterruptibly()
      closed.set(true)
      channel.close()
    } finally {
      lock.unlock()
    }
    println(s"Closed endpoint - ${id} to addr - ${remoteAddrStr}")
  }
}
