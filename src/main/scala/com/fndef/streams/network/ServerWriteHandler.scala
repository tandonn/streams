package com.fndef.streams.network

import java.util
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

abstract class ServerWriteHandler[T](id: String) extends ChannelDataHandler(id) {

  private[this] val endpoints: util.Map[String, EndpointTarget] = new ConcurrentHashMap()
  private[this] val responseQueue: BlockingQueue[T] = new LinkedBlockingQueue[T]()
  override def registerChannel(endpointTarget: EndpointTarget): EndpointTarget = {
    if (isActive) endpoints.put(endpointTarget.endpoint.id, endpointTarget)
    else println(s"Endpoint will not register for writes. Server write handler [${id}] is not active")
    endpointTarget
  }

  def enqueue(t : T): Unit = {
    if (isActive) {
      println(s"enqueuing response - ${t}")
      responseQueue.put(t)
    }
    else println(s"Ignoring write. Server write handler [${id}] is not active")
  }

  override def startInternal: Boolean = {
    start()
    true
  }

  override def stopInternal: Boolean = {
    @tailrec
    def stopRegistered(endpointIterator: util.Iterator[EndpointTarget]): Unit = {
      if (endpointIterator.hasNext) {
        val endpointTarget = endpointIterator.next()
        Try(endpointTarget.endpoint.close) match {
          case Success(_) => println(s"Endpoint closed - [${endpointTarget.endpoint}]")
          case Failure(e) =>
            println(s"Endpoint did not close cleanly")
            e.printStackTrace()
        }
        stopRegistered(endpointIterator)
      }
    }

    println(s"Server write response queue has [${responseQueue.size()}] pending elements which will not be processed.")
    stopRegistered(endpoints.values().iterator())
    true
  }

  def resolveEndpointId(t :T): Option[String]

  override def handlerLoop: Unit = {
    println(s"Starting server write handler [${id}] loop")
    writeLoop
  }

  @tailrec
  private def writeLoop: Unit = {
    if (isActive) {
      Option(responseQueue.poll(10L, TimeUnit.MILLISECONDS)) match {
        case Some(response) =>
          resolveEndpointTarget(response) match {
            case Some(endpointTarget) =>
              handleSelected(response, endpointTarget)
            case None =>
              println(s"Ignoring response. No endpoint found for response - ${response}")
          }
        case None =>
          // loop again
      }
      writeLoop
    }
  }

  private def resolveEndpointTarget(t : T): Option[EndpointTarget] = {
    resolveEndpointId(t) match {
      case Some(endpointId) =>
        Option(endpoints.get(endpointId))
      case _ =>
        None
    }
  }

  def handleSelected(t: T, endpointTarget: EndpointTarget): Unit
}
