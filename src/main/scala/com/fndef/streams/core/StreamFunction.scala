package com.fndef.streams.core

import java.util.UUID

trait StreamFunction extends StreamSource with StreamSink {
  val id: UUID = UUID.randomUUID()
  val name: String
}

