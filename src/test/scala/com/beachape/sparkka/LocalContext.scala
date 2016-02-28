package com.beachape.sparkka

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkContext, SparkConf }

/**
 * Created by Lloyd on 2/28/16.
 */
object LocalContext {

  // http://spark.apache.org/docs/latest/programming-guide.html#initializing-spark
  private val conf = new SparkConf().setAppName("sparka-test").setMaster("local[3]")

  val sc: SparkContext = new SparkContext(conf)

  val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))

}