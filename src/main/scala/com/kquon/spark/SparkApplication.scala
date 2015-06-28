package com.kquon.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import com.kquon.spark.processor.EventProcessor
import com.kquon.spark.ingestor.FlumeIngestor
import com.kquon.spark.repository._

/**
 * Created by kquon on 6/27/15.
 */
object SparkApplication {

  val logger = Logger.getLogger(getClass().getName())

  val APP_NAME = "SparkDataCleanser"
  val MASTER_URL = "local[2]"
  val EXECUTOR_MEMORY = "spark.executor.memory"
  val MEMORY_1GB = "1g"

  val ZOOKEEPER_URL = "localhost:2181"
  val GROUP_ID = "test-consumer-group"
  val TOPICS = scala.collection.immutable.Map[String, Int]("test" -> 1)

  val FLUME_RAW_EVENTS_TEST_URL = "localhost:44448"

  val eventProcessor = new EventProcessor()
  val dataIngestor = new FlumeIngestor()
  val jdbcDAO = new SparkJDBCInsertDAO(new DBProperties(
    "jdbc:postgresql://localhost:5439/dev",
    "root",
    "1234"
  ))

  val EVENT_ASSESSMENT = "AssessmentEvent"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER_URL).set(EXECUTOR_MEMORY, MEMORY_1GB)
    val ssc = new StreamingContext(conf, Seconds(10))
    val messageStream = KafkaUtils.createStream(ssc, ZOOKEEPER_URL, GROUP_ID, TOPICS, StorageLevel.MEMORY_AND_DISK)

    // Raw Events DataStream
    val lines = messageStream.map(_._2)

    // HDFS Ingestion for Raw Events
    lines.foreachRDD(rdd => rdd.foreach(data => dataIngestor.submit(data.asInstanceOf[String], FLUME_RAW_EVENTS_TEST_URL)))

    // Cleaned Events DataStream
    val events = lines.map(line => eventProcessor.process(line))

    // JDBC batch inserts
    jdbcDAO.insert(events)

    events.print()

    ssc.start()
    ssc.awaitTermination()
  }

}