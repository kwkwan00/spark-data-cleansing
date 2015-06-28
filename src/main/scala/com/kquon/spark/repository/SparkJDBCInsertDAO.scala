package com.kquon.spark.repository

import java.util.Date

import java.sql.DriverManager
import java.sql.Timestamp

import org.apache.log4j.Logger

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.streaming.dstream.DStream

/**
 * Created by kquon on 6/27/15.
 */
class SparkJDBCInsertDAO(dbProperties: DBProperties) extends java.io.Serializable {

  Class.forName("org.postgresql.Driver")

  val ASSESSMENT_EVENT_SQL = "INSERT INTO TABLE (CREATED_TS, TYPE, ACTION, USER_ID) VALUES (?, ?, ?, ?)"

  lazy val logger = Logger.getLogger(this.getClass.getName)

  def insert(input: DStream[String]) {
    input.foreachRDD(rdd => {
      val connection = DriverManager.getConnection(dbProperties.url, dbProperties.dbProperties)
      connection.setAutoCommit(false)
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      try {
        val statement = connection.prepareStatement(ASSESSMENT_EVENT_SQL)
        rdd.collect().foreach(data => {
          val event = mapper.readValue[Map[String, Object]](data)
          statement.setTimestamp(1, new Timestamp(new Date().getTime()))
          statement.setString(2, event.get("type").getOrElse("").asInstanceOf[String])
          statement.setString(3, event.get("action").getOrElse("").asInstanceOf[String])
          statement.setString(4, event.get("user.id").getOrElse("").asInstanceOf[String])
          statement.addBatch()
        })
        statement.executeBatch()
        connection.commit()
        statement.close()
      } catch {
        case e: Exception => {
          logger.error("Execution of SQL Statement Failed")
          e.printStackTrace()
          connection.rollback()
        }
      } finally {
        connection.close()
      }
    })
  }
}