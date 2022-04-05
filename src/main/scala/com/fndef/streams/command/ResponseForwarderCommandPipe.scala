package com.fndef.streams.command
import com.fndef.streams.command.response.{CommandResponse, CommandResponseForwarder}
import com.fndef.streams.core.Pipe

trait ResponseForwarderCommandPipe extends CommandPipeWithResponse {
  private[this] val responseForwarder = new CommandResponseForwarder(s"${id}-cmd-response-forwarder")

  override def responsePipe: Pipe[CommandResponse] = responseForwarder
}
