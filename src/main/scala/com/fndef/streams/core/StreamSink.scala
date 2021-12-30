package com.fndef.streams.core

import scala.collection.mutable.Map

trait StreamSink {
  def processEvent(event: EventInternal)(implicit context: ProcessingContext): EventInternal
  def accepts(event: EventInternal): Boolean = Option(event).nonEmpty
}

case class ContextGroup(group: Seq[String], attributeName: String)

protected[core] class ProcessingContext(private[this] val contextAttributes: Map[ContextGroup, EventAttribute]) {
  var currentGroup: Seq[String] = Seq()

  def getAttribute(group: ContextGroup): Option[EventAttribute] = contextAttributes.get(group)

  def addAttribute(group: ContextGroup, attr: EventAttribute): EventAttribute = {
    require(Option(attr).nonEmpty, "Attribute is null")
    contextAttributes += (group -> attr)
    attr
  }

  def removeAttribute(group: ContextGroup): Option[EventAttribute] = {
    require(group != null, "Context group is null")
    contextAttributes.remove(group)
  }

  def clearContext(): ProcessingContext = {
    contextAttributes.clear()
    this
  }

  def containsAttribute(group: ContextGroup): Boolean = {
    require(group != null, "Context group is null")
    contextAttributes.contains(group)
  }

  def contextGroups: Set[ContextGroup] = contextAttributes.keySet.toSet

  def contextAttributes(): Seq[EventAttribute] = contextAttributes.map(_._2).toSeq
}

/*
protected[core] class ProcessingContext(private[this] val contextAttributes:Map[String, EventAttribute]) {

  def getAttribute(name: String): Option[EventAttribute] = contextAttributes.get(name)

  def addAttribute(attr: EventAttribute): EventAttribute = {
    require(Option(attr).nonEmpty, "Attribute is null")
    contextAttributes += (attr.name -> attr)
    attr
  }

  def removeAttribute(name: String): Option[EventAttribute] = {
    require(name != null, "Attribute name is null")
    contextAttributes.remove(name)
  }

  def clearContext(): ProcessingContext = {
    contextAttributes.clear()
    this
  }

  def containsAttribute(name: String): Boolean = {
    require(name != null, "Attribute name is null")
    contextAttributes.contains(name)
  }

  def contextAttributes(): Seq[EventAttribute] = {
    contextAttributes.map(_._2).toSeq
  }
}
*/
object ProcessingContext {
  def apply(): ProcessingContext = {
    new ProcessingContext(Map())
  }
}
