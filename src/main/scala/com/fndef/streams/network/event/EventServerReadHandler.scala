package com.fndef.streams.network.event

import com.fndef.streams.network.{EndpointTarget, ServerReadHandler}

class EventServerReadHandler(id: String, server: EventTcpServer) extends ServerReadHandler(id) {
  override def handleSelected(endpointTarget: EndpointTarget): Unit = ???
}
