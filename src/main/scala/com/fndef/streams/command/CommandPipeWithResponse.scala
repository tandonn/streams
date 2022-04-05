package com.fndef.streams.command

import com.fndef.streams.command.response.WithCommandResponsePipe
import com.fndef.streams.core.IdentifiableSink

trait CommandPipeWithResponse extends CommandPipe with WithCommandResponsePipe {

  override def registerSink[U <: IdentifiableSink[Command]](streamSink: U): Unit = {
    super.registerSink(streamSink)
    streamSink match {
      case withResponseSource: WithCommandResponsePipe =>
        withResponseSource.responsePipe.registerSink(responsePipe)
        println(s"Registered with source [${withResponseSource.responsePipe.id} for command responses")
      case _ =>
        println(s"Command sink [${streamSink.id} does not generate command responses")
    }
  }

  override def deregisterSink[U <: IdentifiableSink[Command]](streamSink: U): Unit = {
    super.deregisterSink(streamSink)
    streamSink match {
      case withResponseSource: WithCommandResponsePipe =>
        withResponseSource.responsePipe.deregisterSink((responsePipe))
        println(s"De-registered with source [${withResponseSource.responsePipe.id} for command responses")
      case _ =>
        println(s"Command sink [${streamSink.id} has no command responses - nothing to de-register")
    }
  }

  override def clearSinks: Unit = {
    def deregisterForResponses(s: IdentifiableSink[Command]): Unit = {
      s match {
        case withResponseSource: WithCommandResponsePipe =>
          withResponseSource.responsePipe.deregisterSink((responsePipe))
          println(s"De-registered with source [${withResponseSource.responsePipe.id} for command responses before clearing sinks")
        case _ =>
          println(s"Command sink [${s.id} has no command responses - nothing to de-register before clearing sinks")
      }
    }
    sinks.foreach(s => deregisterForResponses(s))
    super.clearSinks
  }
}
