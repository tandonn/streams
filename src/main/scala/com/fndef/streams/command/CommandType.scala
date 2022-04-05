package com.fndef.streams.command

sealed abstract class CommandType(val typeId: Int)

case object ParameterizedCommandType extends CommandType(0)
case object CreatePipelineCommandType extends CommandType(1)
case object StartPipelineCommandType extends CommandType(2)
case object StopPipelineCommandType extends CommandType(3)
case object DropPipelineCommandType extends CommandType(4)
case object CommandResponseType extends CommandType(1000)

object CommandType {
  def apply(typeId: Int): CommandType ={
    typeId match {
      case ParameterizedCommandType.typeId =>
        ParameterizedCommandType
      case CreatePipelineCommandType.typeId =>
        CreatePipelineCommandType
      case StartPipelineCommandType.typeId =>
        StartPipelineCommandType
      case StopPipelineCommandType.typeId =>
        StopPipelineCommandType
      case DropPipelineCommandType.typeId =>
        DropPipelineCommandType
      case CommandResponseType.typeId =>
        CommandResponseType
    }
  }
}