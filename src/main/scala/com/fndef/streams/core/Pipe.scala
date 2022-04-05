package com.fndef.streams.core

trait Pipe[T] extends IdentifiableSource[T] with IdentifiableSink[T] with SinkRegistry[T] {
  def publish(t :T): Unit = {
    sinks.foreach(_.process(t))
  }

}