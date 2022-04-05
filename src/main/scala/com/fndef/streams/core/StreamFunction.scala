package com.fndef.streams.core

import java.util.UUID

trait StreamFunction extends StreamSource with StreamSink {
  val id: String = UUID.randomUUID().toString
  val name: String
}

