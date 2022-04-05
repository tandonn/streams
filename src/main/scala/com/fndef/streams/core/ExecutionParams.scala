package com.fndef.streams.core

abstract class ResponseCriteria
case class WithResponse(responseEndpointId: String, timeout: Long = -1, incrementalResponse: Boolean = false) extends ResponseCriteria
case object WithoutResponse extends ResponseCriteria

case class ExecutionParams(responseCriteria: ResponseCriteria = WithoutResponse, maxRetry: Int = 0, highPriority: Boolean = false)