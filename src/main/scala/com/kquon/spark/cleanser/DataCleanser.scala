package com.kquon.spark.cleanser

import scala.collection.mutable.Map

/**
 * Created by kquon on 6/27/15.
 */
trait DataCleanser {

  def execute(data: Map[String, Object]): Unit

}
