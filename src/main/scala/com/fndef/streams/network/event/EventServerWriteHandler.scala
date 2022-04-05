package com.fndef.streams.network.event

import com.fndef.streams.command.response.CommandResponse
import com.fndef.streams.network.{EndpointTarget, ServerReadHandler, ServerWriteHandler}

class EventServerWriteHandler(id: String, server: EventTcpServer) extends ServerWriteHandler[CommandResponse](id) {
  override def resolveEndpointId(t: CommandResponse): Option[String] = ???

  override def handleSelected(t: CommandResponse, endpointTarget: EndpointTarget): Unit = ???
}
