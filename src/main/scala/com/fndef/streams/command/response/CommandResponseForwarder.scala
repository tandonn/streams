package com.fndef.streams.command.response

import com.fndef.streams.core.Pipe

class CommandResponseForwarder(val id: String) extends CommandResponsePipe with WithCommandResponsePipe {

  override def process(response: CommandResponse): Unit = {
    println(s"received command response - ${response}")
    publish(response)
  }

  override def responsePipe: Pipe[CommandResponse] = this
}
