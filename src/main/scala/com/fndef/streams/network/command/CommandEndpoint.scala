package com.fndef.streams.network.command

import java.io.IOException
import java.nio.channels.SocketChannel

import com.fndef.streams.command.Command
import com.fndef.streams.command.response.CommandResponse
import com.fndef.streams.network.ChannelEndpoint

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class CommandEndpoint(id: String, channel: SocketChannel) extends ChannelEndpoint(id, channel) {
  private val cmdProtocolConverter: V1CommandProtocolConverter = new V1CommandProtocolConverter((this))
  private val cmdResponseProtocolConverter = new V1CommandResponseProtocolConverter(this)

  def writeCommandResponse(response: CommandResponse): Unit = {
    cmdResponseProtocolConverter.serialize(response)
  }

  def readCommands: Seq[Command] = {
    Try(resolveCommands(Vector())) match {
      case Success(cmds) =>
        cmds
      case Failure(ex) =>
        ex match {
          case _: IOException =>
            close
            Seq()
          case _ =>
            println(s"Command resolution failed - ${ex.getMessage}")
            Seq()
        }
    }
  }

  @tailrec
  private def resolveCommands(commands: Seq[Command]): Seq[Command] = {
    cmdProtocolConverter.deserialize match {
      case Some(c) =>
        println(s"append command ${c}")
        resolveCommands(commands :+ c)
      case None =>
        println(s"resolved commands - ${commands}")
        commands
    }
  }
}