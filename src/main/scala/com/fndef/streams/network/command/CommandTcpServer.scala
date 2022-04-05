package com.fndef.streams.network.command

import java.nio.channels.SocketChannel
import java.util.concurrent.atomic.AtomicLong

import com.fndef.streams.command.response.CommandResponse
import com.fndef.streams.command.{Command, CommandPipeWithResponse}
import com.fndef.streams.network.{ChannelEndpoint, ServerType, TcpSocketServer}

case object CommandServer extends ServerType

class CommandTcpServer(id: String, port: Int, maxRestart: Int = 3, description: String = "Command server") extends TcpSocketServer[CommandResponse](id, port, CommandServer, maxRestart, description) with CommandPipeWithResponse {
  private[this] val idCounter = new AtomicLong(0)

  override private[network] def createServerReadHandler = new CommandServerReadHandler(s"${id}-read-handler", this)
  override private[network] def createServerWriteHandler = new CommandServerWriteHandler(s"${id}-write-handler")

  override private[network] def createEndpoint(channel: SocketChannel): ChannelEndpoint = {
    channel.configureBlocking(false)
    new CommandEndpoint(s"${id}-${idCounter.incrementAndGet()}", channel)
  }

  override def process(cmd: Command): Unit = publish(cmd)
}
