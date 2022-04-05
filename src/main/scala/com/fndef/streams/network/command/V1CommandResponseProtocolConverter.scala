package com.fndef.streams.network.command

import java.nio.ByteBuffer

import com.fndef.streams.command.{CommandResponseType, CommandType}
import com.fndef.streams.command.response.{CommandExecutionResponse, CommandResponse, EndpointCommandResponse}
import com.fndef.streams.core.ResponseStatus
import com.fndef.streams.network.{ChannelEndpoint, ProtocolConverter}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class V1CommandResponseProtocolConverter(endpoint: ChannelEndpoint) extends ProtocolConverter[CommandResponse] {

  private val cmdRespStart = s"command-resp-length=".getBytes
  private val cmdRespTypeRE = """type=(.*)""".r
  private val cmdIdRE = """cmdid=(.*)""".r
  private val respStatusRE = """status=(.*)""".r

  @tailrec
  private def writeBuff(buff: ByteBuffer): Unit = {
    println(s"writing cmd response buffer ${new String(buff.array())} from position - ${buff.position()}")
    if(buff.hasRemaining) {
      endpoint.channel.write(buff)
      writeBuff(buff)
    }
  }

  private def writeCmdExecResponse(cmdExecResponse: CommandExecutionResponse): Unit = {
    val cmdRespHeader = s"type=${CommandResponseType.typeId},cmdid=${cmdExecResponse.correlationId},status=${cmdExecResponse.status.status}"
    val cmdRespBody = s"${cmdExecResponse.commandType.typeId}"
    val cmdRespTrailer = s"#"
    val cmdResp = s"${cmdRespHeader};${cmdRespBody};${cmdRespTrailer}".getBytes
    val buffer = ByteBuffer.allocate(cmdRespStart.length + cmdResp.length + 4)
    buffer.put(cmdRespStart)
    buffer.putInt(cmdResp.length)
    buffer.put(cmdResp)
    buffer.flip()
    writeBuff(buffer)
  }

  /*
  private def writeCommandResponse(response: CommandResponse): Unit = {
    val cmdRespHeader = s"type=${CommandResponseType.typeId},cmdid=${response.correlationId},status=${response.status.status}"
    val cmdRespBody = s"#"
    val cmdRespTrailer = s"#"
    val cmdResp = s"${cmdRespHeader};${cmdRespBody};${cmdRespTrailer}".getBytes
    val buffer = ByteBuffer.allocate(cmdRespStart.length + cmdResp.length + 4)
    buffer.put(cmdRespStart)
    buffer.putInt(cmdResp.length)
    buffer.put(cmdResp)
    buffer.flip()
    writeBuff(buffer)
  }

   */

  override def serialize(response: CommandResponse): Unit = {
    println(s"Serializing command response - ${response}")
    response match {
      case cmdExecResponse: CommandExecutionResponse =>
        writeCmdExecResponse(cmdExecResponse)
      case endpointCmdResp: EndpointCommandResponse =>
        serialize(endpointCmdResp.cmd)
    }
  }

  override def deserialize: Option[CommandResponse] = {
    println("deserializing command response")
    val buffer = ByteBuffer.allocate(cmdRespStart.length + 4)
    val cnt = endpoint.channel.read(buffer)
    println(s"read byte cnt - ${cnt}")
    if (cnt > 0) {
      buffer.flip()
      val start = new Array[Byte](cmdRespStart.length)
      buffer.get(start)
      println(s"start header  - ${new String(start)}")
      if (cmdRespStart.sameElements(start)) {
        val cmdRespLength = buffer.getInt()
        println(s"cmd resp length = ${cmdRespLength}")
        val cmdRespBytes = new Array[Byte](cmdRespLength)
        val data = ByteBuffer.allocate(cmdRespLength)
        endpoint.channel.read(data)
        data.flip()
        data.get(cmdRespBytes)
        val cmdRespStr = new String(cmdRespBytes)
        println(s"cmd response string - ${cmdRespStr}")
        Try(resolve(cmdRespStr.split(';'))) match {
          case Success(v) =>
            v
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

  private def resolve(cmdRespElements: Array[String]): Option[CommandResponse] = {
    require(cmdRespElements.length == 3, s"Invalid command response format over endpoint - ${endpoint} - elements # = ${cmdRespElements.length}")

    val cmdRespHeader = cmdRespElements(0).trim
    val cmdRespTypeRE(cmdRespType) = cmdRespHeader.split(',')(0)
    val cmdIdRE(cmdId) = cmdRespHeader.split(',')(1)
    val respStatusRE(status) = cmdRespHeader.split(',')(2)

    println(s"command response type - ${cmdRespType.toInt} - status - ${status}")
    cmdRespType.toInt match {
      case CommandResponseType.typeId =>
        resolveCommandResponse(cmdId, status, cmdRespElements(1).trim, cmdRespElements(2).trim)
      case _ =>
        println(s"Invalid command response type [${cmdRespType}]")
        None
    }
  }

  private def resolveCommandResponse(cmdId: String, status: String, cmdRespStr: String, cmdRespTrailer: String): Option[CommandResponse] = {
    val cmdTypeStr = cmdRespStr.trim
    Try(CommandExecutionResponse(cmdId, ResponseStatus(status.toInt), CommandType(cmdTypeStr.toInt))) match {
      case Success(resp) =>
        Option(resp)
      case Failure(e) =>
        println(s"Command response resolve failed - command Id [${cmdId}] - status - [${status}] - response - ${cmdRespStr}")
        e.printStackTrace()
        None
    }
  }
}
