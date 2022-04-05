package com.fndef.streams.network.event

import java.nio.channels.SocketChannel
import java.util.concurrent.atomic.AtomicLong

import com.fndef.streams.command.response.CommandResponse
import com.fndef.streams.network.{ChannelEndpoint, ServerType, TcpSocketServer}

case object EventServer extends ServerType

class EventTcpServer(id: String, port: Int, maxRestart: Int = 3, description: String = "Command server") extends TcpSocketServer[CommandResponse](id, port, EventServer, maxRestart, description) {
  private[this] val idCounter = new AtomicLong(0)

  override private[network] def createServerReadHandler = new EventServerReadHandler(s"${id}-read-handler", this)
  override private[network] def createServerWriteHandler = new EventServerWriteHandler(s"${id}-write-handler", this)

  override private[network] def createEndpoint(channel: SocketChannel): ChannelEndpoint = {
    channel.configureBlocking(false)
    new EventEndpoint(s"${id}-${idCounter.incrementAndGet()}", channel)
  }
}