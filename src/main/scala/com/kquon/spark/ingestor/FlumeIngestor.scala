package com.kquon.spark.ingestor


import scala.collection.mutable.ArrayBuffer

import org.apache.log4j.Logger
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.entity.ContentType

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


import com.kquon.spark.model.JSONEvent

/**
 * Created by kquon on 6/27/15.
 */
class FlumeIngestor {

  val TYPE_JSON = "application/json"
  val ENCODE = "UTF-8"

  val logger = Logger.getLogger(getClass().getName())

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def submit(input: String, url: String) {
    try {
      val httpClient = HttpClients.createDefault()

      val post = new HttpPost("http://" + url)
      post.setEntity(createPayload(input))

      val response = httpClient.execute(post)

      response.close()
      httpClient.close()
    } catch {
      case e: Exception => {
        logger.error("Exception occurred during Event Submission")
        e.printStackTrace()
      }
    }
  }

  def createPayload(jsonBody: String): StringEntity = {
    val retVal = new ArrayBuffer[JSONEvent]();
    val event = new JSONEvent()
    event.setBody(jsonBody)
    retVal.append(event)
    return new StringEntity(mapper.writeValueAsString(retVal), ContentType.create(TYPE_JSON, ENCODE));
  }

}