package com.fndef.streams.core

import java.time.LocalDateTime

trait StreamPacket

case class EventBatch(batchTime: LocalDateTime, events: Seq[EventInternal]) extends StreamPacket
case class BatchSucess(batchTime: LocalDateTime) extends StreamPacket
case class BatchError(batchTime: LocalDateTime, message: String, error: Throwable) extends StreamPacket
case class StoreShutdown(time: LocalDateTime, storeName: String) extends StreamPacket
case class PipelineShutdown()