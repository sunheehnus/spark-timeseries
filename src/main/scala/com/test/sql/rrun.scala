package com.test.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.cloudera.sparkts._

import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import com.cloudera.sparkts.DateTimeIndex._

import breeze.linalg._

/**
  * Created by sunheehnus on 16/1/8.
  */
object rrun extends App {
  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val rdd1 = sqlContext.sparkContext.parallelize(1 to 2048, 3)
  val rdd2 = rdd1.map(x => (x.toString, Vector((x to (x + 2048)).map(_.toDouble).toSeq:_*)))
  val start = new DateTime("2010-01-04", UTC)
  val end = start.plusDays(2048)
  val index = uniform(start, end, new DayFrequency(1))
  val rdd3 = new TimeSeriesRDD(index, rdd2)
  var df = rdd3.toInstantsDataFrame(sqlContext)

  val options = Map("frequency" -> "businessDay", "type" -> "observation", "mapSeries" -> "linear")
  val cmpdf = sqlContext.load("com.test.sql", options)

  val t1 = System.currentTimeMillis()
  df.collect
  val t2 = System.currentTimeMillis()
  val t3 = System.currentTimeMillis()
  cmpdf.collect
  val t4 = System.currentTimeMillis()
  println(t2 - t1)
  println(t4 - t3)
}
