package com.fndef.streams

import java.util.UUID

import com.fndef.streams.command.response.{CommandExecutionResponse, CommandResponse}
import com.fndef.streams.command.{Command, ResponseForwarderCommandPipe}
import com.fndef.streams.core.Success
import com.fndef.streams.network.command.{CommandInvoker, CommandTcpServer}

object TestTcpServer {
  def main(args: Array[String]): Unit = {
    println("starting test")
    val cmdServer: CommandTcpServer = new CommandTcpServer("s1", 8097, 1)
    cmdServer.startup

    val cmdInvoker:CommandInvoker = new CommandInvoker("cmd-invoker1")
    cmdInvoker.startup

    println(s"registering invoker with command server")
    cmdServer.registerSink(cmdInvoker)
    cmdServer.sinks.foreach(s => println(s"ccmd server sinks ${s.id}"))
    println(s"ivoker resposne sinks")
    cmdInvoker.responsePipe.sinks.foreach(s => println(s"ivoker resposne sinks - ${s.id}"))

    // TCP server -> cmd invoker -> component that handles the commands

    // receive commands to execute and send back responses
    cmdInvoker.registerSink(new ResponseForwarderCommandPipe() {
      val id=UUID.randomUUID().toString
      override def process(cmd: Command): Unit = {
        println(s"received cmd to execute - ${cmd}")
        // send back response after execution
        responsePipe.sinks.foreach(_.process(CommandExecutionResponse(cmd.id, Success, cmd.commandType)))
      }
    })

    Thread.sleep(50000)
    cmdServer.shutdown
    cmdInvoker.shutdown
    println("started test - done")
  }
}
