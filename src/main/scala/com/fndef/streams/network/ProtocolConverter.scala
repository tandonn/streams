package com.fndef.streams.network

trait ProtocolConverter[T] {
  def serialize(message: T): Unit
  def deserialize: Option[T]
}
