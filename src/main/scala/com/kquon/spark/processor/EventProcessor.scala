package com.kquon.spark.processor

import scala.collection.mutable.Map
import org.apache.log4j.Logger
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.kquon.spark.cleanser._

/**
 * Created by kquon on 6/27/15.
 */
class EventProcessor {

  val logger = Logger.getLogger(getClass().getName())
  
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  
  val eventCleansers = scala.collection.immutable.Map[String, DataCleanser] (
      "OTHER" -> new DefaultDataCleanser()
  )
  
  def process(line: String): String = {
    try {
      val eventData = mapper.readValue[Map[String, Object]](line)
      val eventType = eventData.getOrElse("type", "").asInstanceOf[String]
      val cleanser = eventCleansers.getOrElse(eventType, new DefaultDataCleanser())
      cleanser.execute(eventData)
      return mapper.writeValueAsString(eventData)
    } catch {
      case e: Exception => {
        logger.error("Exception occurred during Event Processing")
        e.printStackTrace()
      }
    }
    return null
  }
  
  def checkEventType(line: String, eventType: String): Boolean = {
    val eventData = mapper.readValue[Map[String, Object]](line)
    return eventType.equalsIgnoreCase(eventData.getOrElse("type", "").asInstanceOf[String])
  }
  
}