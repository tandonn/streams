package com.fndef.streams

import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.util.UUID

import com.fndef.streams.command.CreatePipelineCommand
import com.fndef.streams.network.command.{CommandEndpoint, V1CommandProtocolConverter, V1CommandResponseProtocolConverter}

object TestTcpClient {
  def main(args: Array[String]): Unit = {
    val channel: SocketChannel = SocketChannel.open()
    channel.connect(new InetSocketAddress("localhost", 8097))
    sendCommand(channel)
    Thread.sleep(50000)
  }

  private def sendCommand(channel: SocketChannel): Unit = {
    val pc = new V1CommandProtocolConverter(new CommandEndpoint("1", channel))
    val resppc = new V1CommandResponseProtocolConverter(new CommandEndpoint("1", channel))
    pc.serialize(CreatePipelineCommand(UUID.randomUUID().toString))
    println("command sent")
    resppc.deserialize match {
      case Some(v) =>
        println(s"Command response deserialized - ${v}")
      case None =>
        println("No command response deserialzied")
    }
  }
}
