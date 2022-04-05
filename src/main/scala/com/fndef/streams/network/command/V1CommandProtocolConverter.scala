package com.fndef.streams.network.command

import java.nio.ByteBuffer

import com.fndef.streams.command.{Command, CreatePipelineCommand, CreatePipelineCommandType, DropPipelineCommand, DropPipelineCommandType, StartPipelineCommand, StartPipelineCommandType, StopPipelineCommand, StopPipelineCommandType}
import com.fndef.streams.network.{ChannelEndpoint, ProtocolConverter}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class V1CommandProtocolConverter(endpoint: ChannelEndpoint) extends ProtocolConverter[Command] {
  private val cmdStart = s"command-length=".getBytes
  private val cmdTypeRE = """type=(.*)""".r
  private val cmdIdRE = """cmdid=(.*)""".r

  override def serialize(command: Command): Unit = {
    @tailrec
    def writeBuff(buff: ByteBuffer): Unit = {
      println(s"writing cmd buffer ${new String(buff.array())}")
      if(buff.hasRemaining) {
        endpoint.channel.write(buff)
        writeBuff(buff)
      }
    }

    val cmdHeader = s"type=${command.commandType.typeId},cmdid=${command.id}"
    val cmdBody = s"${command.commandString}"
    val cmdTrailer = s"#"
    val cmd = s"${cmdHeader};${cmdBody};${cmdTrailer}".getBytes
    val buffer = ByteBuffer.allocate(cmdStart.length + cmd.length + 4)
    buffer.put(cmdStart)
    buffer.putInt(cmd.length)
    buffer.put(cmd)
    buffer.flip()
    writeBuff(buffer)
  }

  override def deserialize: Option[Command] = {
    println("deserializing command")
    val buffer = ByteBuffer.allocate(cmdStart.length + 4)
    val cnt = endpoint.channel.read(buffer)
    println(s"read byte cnt - ${cnt}")
    if (cnt > 0) {
      buffer.flip()
      val start = new Array[Byte](cmdStart.length)
      buffer.get(start)
      println(s"start header  - ${new String(start)}")
      if (cmdStart.sameElements(start)) {
        val cmdLength = buffer.getInt()
        println(s"cmd length = ${cmdLength}")
        val cmdBytes = new Array[Byte](cmdLength)
        val data = ByteBuffer.allocate(cmdLength)
        endpoint.channel.read(data)
        data.flip()
        data.get(cmdBytes)
        val cmdStr = new String(cmdBytes)
        println(s"cmd string - ${cmdStr}")
        Try(resolveCommand(cmdStr.split(';'))) match {
          case Success(v) =>
            Some(v)
          case Failure(exception) =>
            println(s"resolve command failed over endpoint - ${endpoint}")
            exception.printStackTrace()
            None
        }
      } else {
        throw new IllegalArgumentException(s"Invalid command header over endpoint - ${endpoint} - header : ${new String(start)}")
      }
    } else {
      println("no data to read")
      None
    }
  }

  private def resolveCommand(cmdElements: Array[String]): Command = {
    require(cmdElements.length == 3, s"Invalid command format over endpoint - ${endpoint} - elements = ${cmdElements.length}")

    val cmdHeader = cmdElements(0).trim
    val cmdTypeRE(cmdType) = cmdHeader.split(',')(0)
    val cmdIdRE(cmdId) = cmdHeader.split(',')(1)

    println(s"command type - ${cmdType.toInt}")
    cmdType.toInt match {
      case CreatePipelineCommandType.typeId =>
        resolveCreatePipeline(cmdId, cmdElements(1).trim)
      case StartPipelineCommandType.typeId =>
        resolveStartPipeline(cmdId, cmdElements(1).trim)
      case StopPipelineCommandType.typeId =>
        resolveStopPipeline(cmdId, cmdElements(1).trim)
      case DropPipelineCommandType.typeId =>
        resolveDropPipeline(cmdId, cmdElements(1).trim)
    }

  }

  private def resolveDropPipeline(cmdId: String, cmdStr: String): DropPipelineCommand = {
    DropPipelineCommand()
  }

  private def resolveStopPipeline(cmdId: String, cmdStr: String): StopPipelineCommand = {
    StopPipelineCommand()
  }

  private def resolveStartPipeline(cmdId: String, cmdStr: String): StartPipelineCommand = {
    StartPipelineCommand()
  }

  private def resolveCreatePipeline(cmdId: String, cmdStr: String): CreatePipelineCommand = {
    println(s"resolved command - ${CreatePipelineCommand(cmdId)}")
    CreatePipelineCommand(cmdId)
  }
}