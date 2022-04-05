package com.fndef.streams.network.command

import com.fndef.streams.command.response.{CommandExecutionResponse, CommandResponse, EndpointCommandResponse}
import com.fndef.streams.network.{EndpointTarget, ServerWriteHandler}

class CommandServerWriteHandler(id: String) extends ServerWriteHandler[CommandResponse](id) {

  override def handleSelected(response: CommandResponse, endpointTarget: EndpointTarget): Unit = {
    endpointTarget.endpoint match {
      case ep : CommandEndpoint =>
        println(s"Server write handler [${id}] returning response to endpoint [${ep.id}]")
        ep.writeCommandResponse(response)
      case _ =>
        println(s"Unsupported endpoint for command response - ${endpointTarget.endpoint}")
    }
  }

  override def resolveEndpointId(response: CommandResponse): Option[String] = {
    Option(response) match {
      case Some(r) =>
        r match {
          case endpointResponse : EndpointCommandResponse =>
            Option(endpointResponse.endpointId)
          case execResponse : CommandExecutionResponse =>
            Option(execResponse.responseMetadata.responseEndpointId)
          case _ =>
            None
        }
      case None =>
        None
    }
  }
}
