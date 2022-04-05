package com.fndef.streams.core

case class ExecutionTime(startTime: Long, endTime: Long)
case class ResponseMetadata(responseEndpointId: String= "", requestRunnerId: String= "", execTime: ExecutionTime = ExecutionTime(-1,-1), retryCnt: Int = 0)
