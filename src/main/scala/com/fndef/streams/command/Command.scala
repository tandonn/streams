package com.fndef.streams.command

import java.util.UUID

import com.fndef.streams.core.{ExecutionParams, StreamPacket}

abstract class Command(val commandType: CommandType, val id: String = UUID.randomUUID().toString) extends StreamPacket {
  def commandString: String = "-"
}

case class CreatePipelineCommand(commandId: String) extends Command(CreatePipelineCommandType, commandId)
case class StartPipelineCommand() extends Command(StartPipelineCommandType)
case class StopPipelineCommand() extends Command(StopPipelineCommandType)
case class DropPipelineCommand() extends Command(DropPipelineCommandType)

case class ParameterizedCommand(execParams: ExecutionParams = ExecutionParams(), cmd: Command) extends Command(ParameterizedCommandType, cmd.id)