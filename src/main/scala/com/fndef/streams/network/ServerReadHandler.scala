package com.fndef.streams.network

import java.nio.channels.{SelectionKey, Selector}
import java.util.Iterator

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

abstract class ServerReadHandler(id: String) extends ChannelDataHandler(id) {

  lazy private[this] val selector: Selector = Selector.open()

  private def selectionOp: Int = SelectionKey.OP_READ

  override def startInternal: Boolean = {
    start()
    true
  }

  override def stopInternal: Boolean = {
    Try(closeConnections) match {
      case Success(_) =>
        true
      case Failure(e) =>
        e.printStackTrace()
        false
    }
  }

  private def closeConnections: Unit = {
    try {
      val keys = selector.keys()
      iterate(keys.iterator()) {
        key =>
          val endpointTarget = key.attachment().asInstanceOf[EndpointTarget]
          println(s"closing client channel")
          endpointTarget.endpoint.close
      }
    } finally {
      selector.close()
    }
  }

  def handleSelected(endpointTarget: EndpointTarget): Unit

  override def handlerLoop: Unit = {
    println(s"Starting server read handler [${id}] loop")
    readLoop
  }

  @tailrec
  private def iterate(iterator: Iterator[SelectionKey])(handler: SelectionKey => Unit): Unit = {
    if (iterator.hasNext) {
      Try(handler(iterator.next())) match {
        case Failure(e) =>
          e.printStackTrace()
          println(s"failed processing read select event - ${e.getMessage}")
        case Success(_) =>
          println("processed successfully")
      }
      iterator.remove()
      iterate(iterator)(handler)
    }
  }

  @tailrec
  private def readLoop: Unit = {
    if (isActive) {
      val selected = selector.select(10)
      if (selected > 0) {
        println("has read data ready channel")
        val selectedIt = selector.selectedKeys().iterator()
        iterate(selectedIt) {
          selectionKey =>
            if (selectionKey.isReadable) {
              println(s"channel is readable - ${selectionKey.attachment()}")
              handleSelected(selectionKey.attachment().asInstanceOf[EndpointTarget])
            }
        }
      }
      readLoop
    }
  }

  def registerChannel(endpointTarget: EndpointTarget): EndpointTarget = {
    if (isActive) {
      println(s"registering for reads - [${endpointTarget}]")
      endpointTarget.endpoint.channel.register(selector, selectionOp, endpointTarget)
    } else println(s"Endpoint will not register for reads. Server read handler [${id}] is not active.")
    endpointTarget
  }
}

