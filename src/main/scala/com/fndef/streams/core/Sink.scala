package com.fndef.streams.core

import com.fndef.streams.PipelineSpec

import scala.collection.mutable
import scala.collection.mutable.Map

trait Sink[-T] {
  def process(data: T): Unit
}

trait PipelineSink extends Sink[EventInternal] {
  def process(data: EventInternal): Unit
}

trait StreamSink extends Sink[StreamPacket] {
  def process(data: StreamPacket): Unit
}

protected[core] class ProcessingContext(pipelineSpec: PipelineSpec) {
  val groupingContext: GroupingContext = GroupingContext(pipelineSpec.grouping.map(_.opName))
}

case class GroupedAttributeKey(group: Seq[EventAttribute], attributeName: String)

protected[core] class GroupingContext(private[this] val contextAttributes: Map[Seq[EventAttribute], Map[String, EventAttribute]], grouping: Seq[String]) {

  private[this] val groupEvents: Map[Seq[EventAttribute], EventInternal] = mutable.LinkedHashMap()

  def getContextKeys: Seq[Seq[EventAttribute]] = {
    contextAttributes.keySet.toSeq
  }

  private def getAttribute(key: GroupedAttributeKey): Option[EventAttribute] = {
    contextAttributes.get(key.group) match {
      case Some(map) =>
        map.get(key.attributeName)
      case None =>
        None
    }
  }

  def getAttribute(event: EventInternal, attributeName: String): Option[EventAttribute] = {
    getAttribute(GroupedAttributeKey(groupingFor(event), attributeName))
  }

  def groupingFor(event: EventInternal): Seq[EventAttribute] = {
    grouping.map(attr => event.getAttribute(attr).getOrElse(EventAttribute(attr, null)))
  }

  private def addAttribute(key: GroupedAttributeKey, attr: EventAttribute): EventAttribute = {
    require(Option(attr).nonEmpty, "Attribute is null")
    val groupedAttr: Map[String, EventAttribute] = contextAttributes.get(key.group) match {
      case Some(map) =>
        map += (key.attributeName -> attr)
      case None =>
        Map() += (key.attributeName -> attr)
    }
    contextAttributes.put(key.group, groupedAttr)
    attr
  }

  def addAttribute(event: EventInternal, attr: EventAttribute): EventAttribute = {
    require(Option(attr).nonEmpty, "Attribute is null")
    val grouping = groupingFor(event)
    groupEvents.put(grouping, event)
    addAttribute(GroupedAttributeKey(grouping, attr.name), attr)
  }

  def removeAttribute(key: GroupedAttributeKey): Option[EventAttribute] = {
    require(key != null, "Context group is null")
    contextAttributes.get(key.group) match {
      case Some(map) =>
        map.remove(key.attributeName)
      case None =>
        None
    }
  }

  def clearGroup(grouping: Seq[EventAttribute]): GroupingContext = {
    groupEvents.remove(grouping)
    contextAttributes.remove(grouping)
    this
  }

  def clearContext(): GroupingContext = {
    groupEvents.clear()
    contextAttributes.clear()
    this
  }

  def containsAttribute(key: GroupedAttributeKey): Boolean = {
    require(key != null, "key is null")
    contextAttributes.get(key.group) match {
      case Some(map) =>
        map.contains(key.attributeName)
      case None =>
        false
    }
  }

  private def getContextEvent(group: Seq[EventAttribute]): Option[EventInternal] = {
    groupEvents.get(group)
  }

  def listContext: Seq[(Seq[EventAttribute], EventInternal, Seq[EventAttribute])] = {
    groupEvents.map(g => (g._1, g._2, getContextAttributes(g._1))).toSeq
  }

  private def getContextAttributes(group: Seq[EventAttribute]): Seq[EventAttribute] = {
    contextAttributes.get(group) match {
      case Some(attributes) =>
        attributes.map(_._2).toSeq
      case None =>
        Seq()
    }
  }

  private def getContextAttributeNames(group: Seq[EventAttribute]): Seq[String] = {
    contextAttributes.get(group) match {
      case Some(attributes) =>
        attributes.map(_._1).toSeq
      case None =>
        Seq()
    }
  }
}

object ProcessingContext {
  def apply(pipelineSpec: PipelineSpec): ProcessingContext = {
    new ProcessingContext(pipelineSpec)
  }
}

object GroupingContext {
  def apply(grouping: Seq[String]): GroupingContext = {
    new GroupingContext(mutable.LinkedHashMap(), grouping)
  }
}
