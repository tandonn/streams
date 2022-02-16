package com.fndef.streams.core

trait Startable {
  def startup: Boolean
  def shutdown: Boolean
  def isActive: Boolean
}
