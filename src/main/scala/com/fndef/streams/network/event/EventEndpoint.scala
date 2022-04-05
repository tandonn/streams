package com.fndef.streams.network.event

import java.nio.channels.SocketChannel

import com.fndef.streams.network.ChannelEndpoint

class EventEndpoint(id: String, channel: SocketChannel) extends ChannelEndpoint(id, channel) {
  // private val protocolConverter: V1CommandProtocolConverter = new V1CommandProtocolConverter((this))

}
