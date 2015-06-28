package com.kquon.spark.repository

import java.util.Properties

/**
 * Created by kquon on 6/27/15.
 */
class DBProperties(dbURL: String, dbUsername: String, dbPassword: String) extends java.io.Serializable {

  val url = dbURL
  // Create properties object for only Database properties
  val dbProperties = new Properties()
  dbProperties.setProperty("user", dbUsername)
  dbProperties.setProperty("password", dbPassword)

}