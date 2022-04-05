package com.fndef.streams.network.command

import com.fndef.streams.command.{Command, ParameterizedCommand}
import com.fndef.streams.core.{ExecutionParams, WithResponse}
import com.fndef.streams.network.{EndpointTarget, ServerReadHandler}

class CommandServerReadHandler(id: String, server: CommandTcpServer) extends ServerReadHandler(id) {
  override def handleSelected(endpointTarget: EndpointTarget): Unit = {
    def dispatch(cmd: Command): Unit = {
      server.sinks.foreach(_.process(ParameterizedCommand(ExecutionParams(WithResponse(endpointTarget.endpoint.id)), cmd)))
    }

    endpointTarget.endpoint match {
      case cmdEndPoint: CommandEndpoint =>
        cmdEndPoint.readCommands.foreach(dispatch(_))
      case x =>
        // this is events
        println(s"this should not happen - invalid command - ${x}")
    }
  }
}
