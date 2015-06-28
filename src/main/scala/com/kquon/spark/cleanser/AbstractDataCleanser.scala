package com.kquon.spark.cleanser

import org.apache.log4j._
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

/**
 * Created by kquon on 6/27/15.
 */
abstract class AbstractDataCleanser extends DataCleanser {

  val logger = Logger.getLogger(getClass().getName())

  def execute(data: Map[String, Object]): Unit = {
    if (data == null) return

    try {
      // Every event requires these fields
      defaultValues(data, "type", "startTs", "endTs", "action", "user", "payload")

      // Remove everything before the # sign in the Action field
      cleanseActionValue(data)

      convertNestedMapsToMutable(data)

      // Every event requires these object fields
      defaultObjectValues(data, "user", "id", "type", "name")

      // For event specific cleansing
      cleanEventSpecificData(data)
    } catch {
      case e: Exception => {
        logger.error("Exception occurred during General Data Cleansing")
        e.printStackTrace()
      }
    }
  }

  def cleanEventSpecificData(data: Map[String, Object]): Unit

  def ifNotExistsCreateKey(data: Map[String, Object], key: String): Unit = {
    if (data != null && !data.contains(key)) {
      data.put(key, null)
    }
  }

  def ifNotExistsCreateMap(data: Map[String, Object], key: String): Unit = {
    if (!data.contains(key) || data.getOrElse(key, null) == null) {
      data.put(key, new HashMap[String, Object]())
    }
  }

  def renameField(data: Map[String, Object], currentName: String, newName: String): Unit = {
    if (data.contains(currentName)) {
      data.put(newName, data.get(currentName))
      data.remove(currentName)
    }
  }

  def cleanseNestedObject(data: Map[String, Object], key: String): Unit = {
    if (data.get(key) != null) {
      val nestedMap = data.getOrElse(key, null).asInstanceOf[Map[String, Object]]
      cleanseUrlValues(nestedMap)
    }
  }

  def cleanseUrlValues(data: Map[String, Object]): Unit = {
    if (data != null) {
      data.keySet.foreach(key => {
        if (data.get(key).getOrElse("").isInstanceOf[String]) {
          val value = data.getOrElse(key, "").asInstanceOf[String]
          if (!value.contains("{") && (value.contains("http://") || value.contains("https://"))) {
            data.put(key, value.substring(value.lastIndexOf("/") + 1))
          }
        }
      })
    }
  }

  def cleanseActionValue(data: Map[String, Object]): Unit = {
    if (data.get("action") != null) {
      val value = data.get("action").getOrElse("").asInstanceOf[String]
      if (value != null && value.contains("#")) {
        data.put("action", value.substring(value.lastIndexOf("#") + 1));
      }
    }
  }

  def defaultValues(data: Map[String, Object], keys: String*): Unit = {
    keys.foreach(key => ifNotExistsCreateKey(data, key))
  }

  def defaultObjectValues(data: Map[String, Object], key: String, nestedKeys: String*): Unit = {
    if (data.get(key) == null) {
      data.put(key, new HashMap[String, Object]());
    }
    val nestedObject = data.get(key).getOrElse(null).asInstanceOf[Map[String, Object]]
    nestedKeys.foreach(nestedKey => ifNotExistsCreateKey(nestedObject, nestedKey))
  }

  def convertNestedMapsToMutable(data: Map[String, Object]): Unit = {
    data.keySet.foreach(key => {
      val fieldValue = data.get(key).getOrElse(null)
      if (fieldValue != null && fieldValue.isInstanceOf[scala.collection.immutable.Map[_,_]]) {
        val nestedMap = fieldValue.asInstanceOf[scala.collection.immutable.Map[String, Object]]
        data.put(key, Map(nestedMap.toSeq: _*))
      }
    })
  }

  def convertImmutableMapToMutable(map: scala.collection.immutable.Map[String, Object]): Map[String, Object] = {
    return Map(map.toSeq: _*)
  }

  def removeAllNestedMaps(data: Map[String, Object]) : Unit = {
    data.keySet.foreach(key => {
      if (data.get(key).getOrElse(null).isInstanceOf[Map[_,_]]) {
        data.remove(key)
      }
    })
  }

}