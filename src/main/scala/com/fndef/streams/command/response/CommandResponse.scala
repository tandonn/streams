package com.fndef.streams.command.response

import com.fndef.streams.command.{Command, CommandType}
import com.fndef.streams.core.{Response, ResponseMetadata, ResponseStatus}

sealed abstract class CommandResponse(commandId: String, executionStatus: ResponseStatus) extends Response(commandId, executionStatus)
case class CommandExecutionResponse(commandId: String, executionStatus: ResponseStatus, commandType: CommandType, responseMetadata: ResponseMetadata = ResponseMetadata(), origCommand: Option[Command] = None) extends CommandResponse(commandId, executionStatus)
case class EndpointCommandResponse(endpointId: String, cmd: CommandResponse) extends CommandResponse(cmd.correlationId, cmd.status)


