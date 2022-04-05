package com.fndef.streams.network.command

import java.util
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, CountDownLatch, LinkedBlockingQueue, TimeUnit}

import com.fndef.streams.command.response.{CommandResponse, CommandResponsePipe, EndpointCommandResponse, WithCommandResponsePipe}
import com.fndef.streams.command.{Command, CommandPipeWithResponse, ParameterizedCommand}
import com.fndef.streams.core.{ExecutionParams, Pipe, Startable, WithResponse, WithoutResponse}
import com.fndef.streams.network.ServerThread

import scala.annotation.tailrec

class CommandInvoker(val id: String = UUID.randomUUID().toString) extends CommandPipeWithResponse with Startable {

  private[this] val commandCorrelation = new ConcurrentHashMap[String, ExecutionParams]()
  private[this] val commandDispatcher = new CommandDispatcher(s"${id}-cmd-dispatcher", this)
  private[this] val commandResponseHandler = new CommandInvocationResponseHandler(s"${id}-response-handler", commandCorrelation)

  override def process(cmd: Command): Unit = {
    if (isActive) {
      Option(cmd) match {
        case Some(c) =>
          c match {
            case ParameterizedCommand(executionParams, command) if executionParams.responseCriteria == WithoutResponse =>
              // no response required
              commandDispatcher.enqueue(command)
            case ParameterizedCommand(executionParams, command) =>
              commandCorrelation.put(command.id, executionParams)
              commandDispatcher.enqueue(c)
            case _ =>
              // no response parameters
              commandDispatcher.enqueue(c)
          }
        case None =>
          println("Null command is ignored")
      }
    } else println(s"Command invoker [${id}] is not active. Command [${cmd}] is ignored.")
  }

  override def startup: Boolean = commandDispatcher.startup

  override def shutdown: Boolean = commandDispatcher.shutdown

  override def isActive: Boolean = commandDispatcher.isActive

  override def responsePipe: Pipe[CommandResponse] = commandResponseHandler
}

private class CommandInvocationResponseHandler(val id: String, commandCorrelation: util.Map[String, ExecutionParams]) extends CommandResponsePipe {

  override def process(response: CommandResponse): Unit = {
    println(s"received command response - ${response}")
    Option(response) match {
      case Some(r) =>
        Option(commandCorrelation.get(r.correlationId)) match {
          case Some(ExecutionParams(WithResponse(responseEndpoint, timeout, incrementalResponse), _, _)) =>
            println(s"Publishing response to endpoint [${responseEndpoint}]")
            commandCorrelation.remove(r.correlationId)
            publish(EndpointCommandResponse(responseEndpoint, r))
          case _ =>
            println(s"No response required. Ignoring response ${response}")
        }
    }
  }
}

private class CommandDispatcher(threadId: String, commandInvoker: CommandInvoker) extends ServerThread(threadId) {

  private[this] val active: AtomicBoolean = new AtomicBoolean(false)
  private[this] val stopped: AtomicBoolean = new AtomicBoolean(false)
  private[this] val lock = new ReentrantLock()
  private[this] val latch = new CountDownLatch(1)
  private[this] val commandQueue: BlockingQueue[Command] = new LinkedBlockingQueue[Command]()
  private[this] val pollTimeout = 10L

  def enqueue(cmd: Command): Unit = {
    commandQueue.put(cmd)
  }

  override def run(): Unit = {
    println(s"read handler [${id}] initialized")
    try {
      active.set(true)
      latch.countDown()
      runLoop
    } finally {
      active.set(false)
      stopped.set(true)
    }
  }

  @tailrec
  private def runLoop: Unit = {
    if (active.get() && !stopped.get()) {
      Option(commandQueue.poll(pollTimeout, TimeUnit.MILLISECONDS))
        .foreach(cmd => commandInvoker.sinks.foreach(_.process(cmd)))
      runLoop
    }
  }

  override def startup: Boolean = {
    try {
      lock.lockInterruptibly()
      require(!stopped.get(), s"Command dispatcher - ${id} is stopped and does not restart")
      start()
      latch.await()
    } finally {
      lock.unlock()
    }
    active.get() && !stopped.get()
  }

  override def shutdown: Boolean = {
    try {
      lock.lockInterruptibly()
      active.set(false)
      stopped.set(true)
    }finally {
      lock.unlock()
    }
    stopped.get()
  }

  override def isActive: Boolean = {
    try {
      lock.lockInterruptibly()
      active.get() && !stopped.get()
    } finally {
      lock.unlock()
    }
  }
}