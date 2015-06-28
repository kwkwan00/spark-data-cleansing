package com.kquon.spark.model

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

/**
 * Created by kquon on 6/27/15.
 */
class JSONEvent {

  var headers: Map[String, String] = new HashMap[String, String]()

  var body: String = null

  def getHeaders(): Map[String, String] = {
    return headers;
  }

  def setHeaders(headers: Map[String, String]) {
    this.headers = headers;
  }

  def getBody(): String = {
    return body;
  }

  def setBody(body: String) {
    this.body = body;
  }

}