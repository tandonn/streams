package com.fndef

import java.time.Duration
import java.time.format.DateTimeFormatter

import com.fndef.streams.core.EventAttribute
import com.fndef.streams.core.operation.{AggregateOp, AndFilter, AttributeOp, EqualFilter, EventAttributeResolver, FilterAttributeResolver, FilterOp, GreaterThan, GreaterThanEqual, IsNullFilter, LessThan, LessThanEqual, LiteralOp, LiteralResolver, NoFilter, NotEqualFilter, NotFilter, NotNullFilter, OrFilter, SelectOp}
import com.fndef.streams.core.function.{Pipeline, WindowedPipeline}


package object streams {
  type Event = com.fndef.streams.core.Event

  def create(pipelineName: String): PipelineSpec = {
    PipelineSpec(pipelineName)
  }

  //def lit(value: Int): EventAttribute = EventAttribute(LiteralOp(value).attributeName, value)
  def lit(value: Long): EventAttribute = EventAttribute(LiteralOp(value).attributeName, value)
  //def lit(value: Float): EventAttribute = EventAttribute(LiteralOp(value).attributeName, value)
  def lit(value: Double): EventAttribute = EventAttribute(LiteralOp(value).attributeName, value)
  def lit(value: Boolean): EventAttribute = EventAttribute(LiteralOp(value).attributeName, value)
  def lit(value: String): EventAttribute = EventAttribute(LiteralOp(value).attributeName, value)

  def windowOver(attribute: String): WindowSpec = {
    WindowSpec(attribute, DateTimeFormatter.ISO_LOCAL_DATE_TIME, Duration.ofMillis(100), Duration.ofMillis(100), Duration.ofMillis(5000), false)
  }

  def windowOver(attribute: String, ofDuration: Duration): WindowSpec = {
    WindowSpec(attribute, DateTimeFormatter.ISO_LOCAL_DATE_TIME, ofDuration, ofDuration, ofDuration.multipliedBy(50), false)
  }

  def windowOf(duration: Duration): WindowSpec = {
    WindowSpec("", DateTimeFormatter.ISO_LOCAL_DATE_TIME, duration, duration, duration.multipliedBy(3), true)
  }

  case class WindowSpec(timeAttribute: String, timeFormat: DateTimeFormatter, windowDuration: Duration, slideBy: Duration, expireBy: Duration, internalTime: Boolean) {
    def over(attribute: String): WindowSpec = {
      WindowSpec(attribute, timeFormat, windowDuration, slideBy, expireBy, false)
    }

    def of(duration: Duration): WindowSpec = {
      WindowSpec(timeAttribute, timeFormat, duration, slideBy, expireBy, internalTime)
    }

    def slideBy(duration: Duration): WindowSpec = {
      WindowSpec(timeAttribute, timeFormat, windowDuration, duration, expireBy, internalTime)
    }

    def timeFormat(eventTimeFormat: String): WindowSpec = {
      WindowSpec(timeAttribute, DateTimeFormatter.ofPattern(eventTimeFormat), windowDuration, slideBy, expireBy, internalTime)
    }

    def timeFormat(eventTimeFormat: DateTimeFormatter): WindowSpec = {
      WindowSpec(timeAttribute, eventTimeFormat, windowDuration, slideBy, expireBy, internalTime)
    }

    def expireBy(expiryDelay: Duration): WindowSpec = {
      WindowSpec(timeAttribute, timeFormat, windowDuration, slideBy, expiryDelay, internalTime)
    }

    def isTumbling: Boolean = {
      windowDuration.equals(slideBy)
    }
  }

  abstract class FilterSpec {
    val attributes: Seq[String]
    val filterOp: FilterOp

    def resolveName(name: String): String = {
      if (AggregateOp.isAggregation(name)) {
        AggregateOp(name).opName
      } else {
        name
      }
    }
  }

  case class AttributeFilterSpec(left: FilterAttributeResolver, attributes: Seq[String] = Seq(), filterOp: FilterOp) extends FilterSpec {
    def lessThan(right: String): CompoundFilterSpec = {
      val rightAttr = resolveName(right)
      val filter = LessThan(left, new EventAttributeResolver(rightAttr))
      CompoundFilterSpec(filter, rightAttr +: attributes, filter)
    }

    def lessThan(lit: EventAttribute): CompoundFilterSpec = {
      val filter = LessThan(left, new LiteralResolver(lit))
      CompoundFilterSpec(filter, attributes, filter)
    }

    def lessThanEqual(right: String): CompoundFilterSpec = {
      val rightAttr = resolveName(right)
      val filter = LessThanEqual(left, new EventAttributeResolver(rightAttr))
      CompoundFilterSpec(filter, rightAttr +: attributes, filter)
    }

    def lessThanEqual(lit: EventAttribute): CompoundFilterSpec = {
      val filter = LessThanEqual(left, new LiteralResolver(lit))
      CompoundFilterSpec(filter, attributes, filter)
    }

    def greaterThan(right: String): CompoundFilterSpec = {
      val rightAttr = resolveName(right)
      val filter = GreaterThan(left, new EventAttributeResolver(rightAttr))
      CompoundFilterSpec(filter, rightAttr +: attributes, filter)
    }

    def greaterThan(lit: EventAttribute): CompoundFilterSpec = {
      val filter = GreaterThan(left, new LiteralResolver(lit))
      CompoundFilterSpec(filter, attributes, filter)
    }

    def greaterThanEqual(right: String): CompoundFilterSpec = {
      val rightAttr = resolveName(right)
      val filter = GreaterThanEqual(left, new EventAttributeResolver(rightAttr))
      CompoundFilterSpec(filter, rightAttr +: attributes, filter)
    }

    def greaterThanEqual(lit: EventAttribute): CompoundFilterSpec = {
      val filter = GreaterThanEqual(left, new LiteralResolver(lit))
      CompoundFilterSpec(filter, attributes, filter)
    }

    def isNotNull: CompoundFilterSpec = {
      val filter = NotNullFilter(left)
      CompoundFilterSpec(filter, attributes, filter)
    }

    def isNull: CompoundFilterSpec = {
      val filter = IsNullFilter(left)
      CompoundFilterSpec(filter, attributes, filter)
    }

    def equalTo(right: String): CompoundFilterSpec = {
      val rightAttr = resolveName(right)
      val filter = EqualFilter(left, new EventAttributeResolver(rightAttr))
      CompoundFilterSpec(filter, rightAttr +: attributes, filter)
    }

    def equalTo(lit: EventAttribute): CompoundFilterSpec = {
      val filter = EqualFilter(left, new LiteralResolver(lit))
      CompoundFilterSpec(filter, attributes, filter)
    }

    def notEqualTo(right: String): CompoundFilterSpec = {
      val rightAttr = resolveName(right)
      val filter = NotEqualFilter(left, new EventAttributeResolver(rightAttr))
      CompoundFilterSpec(filter, rightAttr +: attributes, filter)
    }

    def notEqualTo(lit: EventAttribute): CompoundFilterSpec = {
      val filter = NotEqualFilter(left, new LiteralResolver(lit))
      CompoundFilterSpec(filter, attributes, filter)
    }
  }

  case class CompoundFilterSpec(left: FilterOp, attributes: Seq[String] = Seq(), filterOp: FilterOp) extends FilterSpec {
    def not: CompoundFilterSpec = {
      val filter = NotFilter(left)
      CompoundFilterSpec(filter, attributes, filter)
    }

    def and(filterSpec: CompoundFilterSpec): CompoundFilterSpec = {
      val filter = AndFilter(left, filterSpec.filterOp)
      CompoundFilterSpec(filter, attributes, filter)
    }

    def or(filterSpec: FilterSpec): CompoundFilterSpec = {
      val filter = OrFilter(left, filterSpec.filterOp)
      CompoundFilterSpec(filter, attributes, filter)
    }
  }

  def filterOf(attr: String): AttributeFilterSpec = {
    val attrName: String = if (AggregateOp.isAggregation(attr)) {
      AggregateOp(attr).opName
    } else {
      attr
    }
    AttributeFilterSpec(new EventAttributeResolver(attrName), attrName +: Seq(), NoFilter(attrName))
  }

  def filterOf(literal: EventAttribute): AttributeFilterSpec = {
    AttributeFilterSpec(new LiteralResolver(literal), Seq(), NoFilter(literal.name))
  }

  case class PipelineSpec(pipelineName: String,
                          windowSpec: WindowSpec = WindowSpec("", DateTimeFormatter.ISO_LOCAL_DATE_TIME, Duration.ofMillis(100), Duration.ofMillis(100), Duration.ofMillis(5000), true),
                          selections: Seq[AttributeOp] = Seq(),
                          filters: Seq[FilterOp] = Seq(),
                          grouping: Seq[AttributeOp] = Seq(),
                          having: Seq[FilterOp] = Seq(),
                          literals: Seq[EventAttribute] = Seq(),
                          aggregations: Seq[AggregateOp] = Seq(),
                          applyStrictGrouping: Boolean = false) {


    def windowBy(spec: WindowSpec): PipelineSpec = {
      PipelineSpec(pipelineName, spec, selections, filters, grouping, having, literals, aggregations, applyStrictGrouping)
    }

    def select(attributes: String*): PipelineSpec = {
      PipelineSpec(pipelineName, windowSpec, mergeSelections(attributes), filters, grouping, having, literals, mergeAgg(attributes), applyStrictGrouping)
    }

    def literals(lits: EventAttribute*): PipelineSpec = {
      PipelineSpec(pipelineName, windowSpec, selections, filters, grouping, having, mergeLiterals(lits), aggregations, applyStrictGrouping)
    }

    def filter(filterSpecs: FilterSpec*): PipelineSpec = {
      PipelineSpec(pipelineName, windowSpec, selections, mergeFilters(filters, filterSpecs), grouping, having, literals, aggregations, applyStrictGrouping)
    }

    def groupBy(attributes: String*): PipelineSpec = {
      val aggregationAttrs: Seq[String] = attributes.filter(AggregateOp.isAggregation(_)).map(AggregateOp(_).opName)
      val namedAttrs: Seq[String] = attributes.filterNot(AggregateOp.isAggregation(_))

      PipelineSpec(pipelineName, windowSpec, selections, filters, (aggregationAttrs.map(SelectOp(_)) ++ namedAttrs.map(SelectOp(_))), having, literals, aggregations, applyStrictGrouping)
    }

    def having(attributes: FilterSpec*): PipelineSpec = {
      PipelineSpec(pipelineName, windowSpec, selections, filters, grouping, mergeFilters(having, attributes), literals, mergeAgg(attributes.flatMap(_.attributes)), applyStrictGrouping)
    }

    def strictGrouping(strict: Boolean): PipelineSpec = {
      PipelineSpec(pipelineName, windowSpec, selections, filters, grouping, having, literals, aggregations, strict)
    }

    def start: Pipeline = {
      val pipeline: Pipeline = new WindowedPipeline(this)
      pipeline.startup
      pipeline
    }

    private def mergeFilters(existingfilters: Seq[FilterOp], newFilterSpecs: Seq[FilterSpec]): Seq[FilterOp] = {
      existingfilters ++ newFilterSpecs.map(_.filterOp).filterNot(existingfilters.contains(_))
    }

    private def mergeSelections(attributes: Seq[String]): Seq[AttributeOp] = {
      val aggregations: Seq[String] = attributes.filter(AggregateOp.isAggregation(_)).map(AggregateOp(_).opName)
      val simpleSelects: Seq[String] = attributes.filterNot(AggregateOp.isAggregation(_))
      selections ++ (aggregations.map(SelectOp(_)) ++ simpleSelects.map(SelectOp(_))).filterNot(selections.contains(_))
    }

    private def mergeLiterals(lits: Seq[EventAttribute]): Seq[EventAttribute] = {
      literals ++ lits.filterNot(literals.contains(_))
    }

    private def mergeAgg(attributes: Seq[String]): Seq[AggregateOp] = {
      aggregations ++ attributes.filter(AggregateOp.isAggregation(_)).map(AggregateOp(_)).filterNot(aggregations.contains(_))
    }
  }
}
