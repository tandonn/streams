package com.fndef.streams.core

trait WithResponsePipe[R] {
  def responsePipe: Pipe[R]
}

