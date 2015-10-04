package com.run

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import com.redislabs.provider.redis._


import java.net.InetAddress
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark._
import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._
import com.redislabs.provider.util.ImportTimeSeriesData._
import com.redislabs.provider.util.GenerateWorkdayTestData._
import com.redislabs.provider.redis.partitioner._
import com.redislabs.provider.RedisConfig
import com.redislabs.provider.redis._

import com.cloudera.sparkts._
import com.cloudera.sparkts.DateTimeIndex._

import com.github.nscala_time.time.Imports._

import breeze.linalg._
import breeze.numerics._

import com.cloudera.finance.YahooParser
import com.cloudera.sparkts.DateTimeIndex._
import com.cloudera.sparkts.TimeSeries
import com.cloudera.sparkts.UnivariateTimeSeries._
import com.cloudera.sparkts.TimeSeriesRDD._
import com.cloudera.sparkts.TimeSeriesStatisticalTests._

import com.github.nscala_time.time.Imports._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.joda.time.DateTimeZone.UTC

import java.io._

object Main {
  
  def averageTime(rdd1: RDD[(String, Vector[Double])], rdd2: RDD[(String, Vector[Double])], cnt: Int, writer: java.io.PrintWriter) {
    val startTimerdd1 = System.currentTimeMillis
    (0 until cnt).foreach(x => rdd1.collect())
    val endTimerdd1 = System.currentTimeMillis
    val period1 = (endTimerdd1 - startTimerdd1) / 1000.0 / cnt
    writer.write(f"TimeSeriesRDD: ${period1}%.2f s\n")
    
    val startTimerdd2 = System.currentTimeMillis
    (0 until cnt).foreach(x => rdd2.collect())
    val endTimerdd2 = System.currentTimeMillis
    val period2 = (endTimerdd2 - startTimerdd2) / 1000.0 / cnt
    writer.write(f"RedisTimeSeriesRDD: ${period2}%.2f s\n")
    
    val improve = (period1 - period2) * 100.0 / period1
    writer.write(f"Improved by: ${improve}%.2f %%\n\n\n")
  }
  
  def rddEquals(rdd: RDD[(String, Vector[Double])], Redisrdd: RDD[(String, Vector[Double])]): Boolean = {
    val rdd1collect = rdd.collect()
    val rdd2collect = Redisrdd.collect()
    if (rdd1collect.size != rdd2collect.size) {
      return false
    }
    else {
      for (i <- 0 to (rdd1collect.size - 1).toInt) {
        val arr1 = rdd1collect(i)._2
        val arr2 = rdd2collect.filter(x => {x._1.substring(x._1.indexOf("_") + 1) == rdd1collect(i)._1})(0)._2
        if (arr1.size != arr2.size) {
          return false
        }
        else {
          for (j <- 0 to (arr1.size - 1).toInt) {
            if (abs(arr1(j) - arr2(j)) > 0.01) {
              return false
            }
          }
        }
      }
    }
    return true
  }
  
  def TEST(sc: SparkContext, writer: PrintWriter, cnt: Int, msg: String, dir: String, prefix: String, redisNode: (String, Int)) {
    ImportToRedisServer(dir, prefix, sc, redisNode)
    
    writer.write("****** "+ msg + " ******\n")
    val seriesByFile: RDD[TimeSeries] = YahooParser.yahooFiles(dir, sc)

    val start = seriesByFile.map(_.index.first).takeOrdered(1).head
    val end = seriesByFile.map(_.index.last).top(1).head
    val dtIndex = uniform(start, end, 1.businessDays)
    
    val Rdd = timeSeriesRDD(dtIndex, seriesByFile)
    val cmpRdd = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex)
    if (rddEquals(Rdd, cmpRdd)) {
      writer.write("RDD TEST passed\n")
    }
    else {
      writer.write("RDD TEST failed\n")
      return
    }
    averageTime(Rdd, cmpRdd, cnt, writer)

    val filterRdd = Rdd.filter(_._1.endsWith("Col1"))
    val cmpfilterRdd = sc.fromRedisKeyPattern(redisNode, prefix + "_*Col1").getRedisTimeSeriesRDD(dtIndex)
    if (rddEquals(filterRdd, cmpfilterRdd)) {
      writer.write("Filter by Regex RDD TEST passed\n")
    }
    else {
      writer.write("Filter by Regex RDD TEST failed\n")
      return
    }
    averageTime(filterRdd, cmpfilterRdd, cnt, writer)
    
    var filteredRddStart = filterRdd.filterStartingBefore(start)
    var cmpfilteredRddStart = cmpfilterRdd.filterStartingBefore(start)
    if (rddEquals(filteredRddStart, cmpfilteredRddStart)) {
      writer.write("Filter by StartTime TEST passed\n")
    }
    else {
      writer.write("Filter by StartTime TEST failed\n")
      return
    }
    averageTime(filteredRddStart, cmpfilteredRddStart, cnt, writer)
    
    var filteredRddEnd = filterRdd.filterEndingAfter(end)
    var cmpfilteredRddEnd = cmpfilterRdd.filterEndingAfter(end)
    if (rddEquals(filteredRddEnd, cmpfilteredRddEnd)) {
      writer.write("Filter by EndTime TEST passed\n")
    }
    else {
      writer.write("Filter by EndTime TEST failed\n")
      return
    }
    averageTime(filteredRddEnd, cmpfilteredRddEnd, cnt, writer)

    val _slicest = nextBusinessDay(new DateTime(start.getMillis + (end.getMillis - start.getMillis)/10, UTC)).toString
    val _sliceet = nextBusinessDay(new DateTime(start.getMillis + (end.getMillis - start.getMillis)/5, UTC)).toString
    val slicest = new DateTime(_slicest.toString.substring(0, _slicest.toString.indexOf("T")))
    val sliceet = new DateTime(_sliceet.toString.substring(0, _sliceet.toString.indexOf("T")))
    val slicedRdd = Rdd.slice(slicest, sliceet).fill("linear")
    val cmpslicedRdd = cmpRdd.slice(slicest, sliceet).fill("linear")
    if (rddEquals(filteredRddEnd, cmpfilteredRddEnd)) {
      writer.write("Slice TEST passed\n")
    }
    else {
      writer.write("Slice TEST failed\n")
      return
    }
    averageTime(slicedRdd, cmpslicedRdd, cnt, writer)
    
    writer.write("\n\n\n\n\n")
    writer.flush
  }
  
  def main(args: Array[String]) {
    //Generate("/home/hadoop/RedisLabs/TEST")
    
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    
    val writer = new PrintWriter(new File("result.out"))
    
    (1 to 16).foreach{ i => {
      TEST(sc, writer, 8, "TEST " + i.toString, "/home/hadoop/RedisLabs/TEST/TEST" + i.toString, "TEST" + i.toString, ("127.0.0.1", 7000)) 
    }}
    
    writer.close
  }
}