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
import org.apache.spark.storage.StorageLevel
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
    
    val improve = period1 * 100.0 / period2
    writer.write(f"Speed up: ${improve}%.2f %%\n\n\n")
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
        val arr2 = rdd2collect.filter(x => {x._1 == rdd1collect(i)._1})(0)._2
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
  
  def TEST(sc: SparkContext, writer: PrintWriter, ty: String, cnt: Int, msg: String, dir: String, prefix: String, redisNode: (String, Int)) {
    
    writer.write("****** "+ msg + " ******\n")
    val seriesByFile: RDD[TimeSeries] = YahooParser.yahooFiles(dir, sc)

    val start = seriesByFile.map(_.index.first).takeOrdered(1).head
    val end = seriesByFile.map(_.index.last).top(1).head
    val dtIndex = uniform(start, end, 1.businessDays)
    
    val Rdd = timeSeriesRDD(dtIndex, seriesByFile)
    val cmpRdd = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex)

    if (ty == "SER") {
        Rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
        Rdd.collect
    }
    if (ty == "Tachyon") {
        Rdd.persist(StorageLevel.OFF_HEAP)
        Rdd.collect
    }

    if (rddEquals(Rdd, cmpRdd)) {
        writer.write("RDD TEST passed\n")
    }
    else {
        writer.write("RDD TEST failed\n")
        return
    }
    averageTime(Rdd, cmpRdd, cnt, writer)

    val filterRdd1 = Rdd.filter(_._1.endsWith("Col1"))
    val cmpfilterRdd1 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterKeys(".*Col1")
    if (rddEquals(filterRdd1, cmpfilterRdd1)) {
        writer.write("Filter by Regex RDD TEST passed\n")
    }
    else {
        writer.write("Filter by Regex RDD TEST failed\n")
        return
    }
    averageTime(filterRdd1, cmpfilterRdd1, cnt, writer)
    
    val filterRdd2 = Rdd.filter(_._1.endsWith("Col8"))
    val cmpfilterRdd2 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterKeys(".*Col8")
    if (rddEquals(filterRdd2, cmpfilterRdd2)) {
        writer.write("Filter by Regex RDD TEST passed\n")
    }
    else {
        writer.write("Filter by Regex RDD TEST failed\n")
        return
    }
    averageTime(filterRdd2, cmpfilterRdd2, cnt, writer)
    
    val filterRdd3 = Rdd.filter(_._1.endsWith("Col15"))
    val cmpfilterRdd3 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterKeys(".*Col15")
    if (rddEquals(filterRdd3, cmpfilterRdd3)) {
        writer.write("Filter by Regex RDD TEST passed\n")
    }
    else {
        writer.write("Filter by Regex RDD TEST failed\n")
        return
    }
    averageTime(filterRdd3, cmpfilterRdd3, cnt, writer)

    val startTime1 = new DateTime("1981-10-12", UTC)
    var filteredRddStart1 = Rdd.filterStartingBefore(startTime1)
    var cmpfilteredRddStart1 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterStartingBefore(startTime1)
    if (rddEquals(filteredRddStart1, cmpfilteredRddStart1)) {
        writer.write("Filter by StartTime RDD TEST passed\n")
    }
    else {
        writer.write("Filter by StartTime RDD TEST failed\n")
        return
    }
    averageTime(filteredRddStart1, cmpfilteredRddStart1, cnt, writer)
    
    val startTime2 = new DateTime("1983-10-10", UTC)
    var filteredRddStart2 = Rdd.filterStartingBefore(startTime2)
    var cmpfilteredRddStart2 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterStartingBefore(startTime2)
    if (rddEquals(filteredRddStart2, cmpfilteredRddStart2)) {
        writer.write("Filter by StartTime RDD TEST passed\n")
    }
    else {
        writer.write("Filter by StartTime RDD TEST failed\n")
        return
    }
    averageTime(filteredRddStart2, cmpfilteredRddStart2, cnt, writer)
    
    val startTime3 = new DateTime("1985-10-10", UTC)
    var filteredRddStart3 = Rdd.filterStartingBefore(startTime3)
    var cmpfilteredRddStart3 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterStartingBefore(startTime3)
    if (rddEquals(filteredRddStart3, cmpfilteredRddStart3)) {
        writer.write("Filter by StartTime RDD TEST passed\n")
    }
    else {
        writer.write("Filter by StartTime RDD TEST failed\n")
        return
    }
    averageTime(filteredRddStart3, cmpfilteredRddStart3, cnt, writer)
    
    val endTime1 = new DateTime("2010-11-11", UTC)
    var filteredRddEnd1 = Rdd.filterEndingAfter(endTime1)
    var cmpfilteredRddEnd1 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterEndingAfter(endTime1)
    if (rddEquals(filteredRddEnd1, cmpfilteredRddEnd1)) {
        writer.write("Filter by EndTime RDD TEST passed\n")
    }
    else {
        writer.write("Filter by EndTime RDD TEST failed\n")
        return
    }
    averageTime(filteredRddEnd1, cmpfilteredRddEnd1, cnt, writer)

    val endTime2 = new DateTime("2013-11-11", UTC)
    var filteredRddEnd2 = Rdd.filterEndingAfter(endTime2)
    var cmpfilteredRddEnd2 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterEndingAfter(endTime2)
    if (rddEquals(filteredRddEnd2, cmpfilteredRddEnd2)) {
        writer.write("Filter by EndTime RDD TEST passed\n")
    }
    else {
        writer.write("Filter by EndTime RDD TEST failed\n")
        return
    }
    averageTime(filteredRddEnd2, cmpfilteredRddEnd2, cnt, writer)
    
    val endTime3 = new DateTime("2015-11-11", UTC)
    var filteredRddEnd3 = Rdd.filterEndingAfter(endTime3)
    var cmpfilteredRddEnd3 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterEndingAfter(endTime3)
    if (rddEquals(filteredRddEnd3, cmpfilteredRddEnd3)) {
        writer.write("Filter by EndTime RDD TEST passed\n")
    }
    else {
        writer.write("Filter by EndTime RDD TEST failed\n")
        return
    }
    averageTime(filteredRddEnd3, cmpfilteredRddEnd3, cnt, writer)
    
    val slicest1 = new DateTime("2010-10-11", UTC)
    val sliceet1 = new DateTime("2012-10-09", UTC)
    val slicedRdd1 = Rdd.slice(slicest1, sliceet1).fill("linear")
    val cmpslicedRdd1 = cmpRdd.slice(slicest1, sliceet1).fill("linear")
    if (rddEquals(slicedRdd1, cmpslicedRdd1)) {
        writer.write("Slice RDD TEST passed\n")
    }
    else {
        writer.write("Slice RDD TEST failed\n")
        return
    }
    averageTime(slicedRdd1, cmpslicedRdd1, cnt, writer)

    val slicest2 = new DateTime("2012-07-16", UTC)
    val sliceet2 = new DateTime("2012-10-15", UTC)
    val slicedRdd2 = Rdd.slice(slicest2, sliceet2).fill("linear")
    val cmpslicedRdd2 = cmpRdd.slice(slicest2, sliceet2).fill("linear")
    if (rddEquals(slicedRdd2, cmpslicedRdd2)) {
        writer.write("Slice RDD TEST passed\n")
    }
    else {
        writer.write("Slice RDD TEST failed\n")
        return
    }
    averageTime(slicedRdd2, cmpslicedRdd2, cnt, writer)

    val slicest3 = new DateTime("2011-10-03", UTC)
    val sliceet3 = new DateTime("2011-10-28", UTC)
    val slicedRdd3 = Rdd.slice(slicest3, sliceet3).fill("linear")
    val cmpslicedRdd3 = cmpRdd.slice(slicest3, sliceet3).fill("linear")
    if (rddEquals(slicedRdd3, cmpslicedRdd3)) {
        writer.write("Slice RDD TEST passed\n")
    }
    else {
        writer.write("Slice RDD TEST failed\n")
        return
    }
    averageTime(slicedRdd3, cmpslicedRdd3, cnt, writer)
    
    val fsRdd1 = Rdd.filterStartingBefore(slicest1).filterEndingAfter(sliceet1).slice(slicest1, sliceet1).fill("linear")
    val cmpfsRdd1 = cmpRdd.filterStartingBefore(slicest1).filterEndingAfter(sliceet1).slice(slicest1, sliceet1).fill("linear")
    if (rddEquals(fsRdd1, cmpfsRdd1)) {
        writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed\n")
    }
    else {
        writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed\n")
        return
    }
    averageTime(fsRdd1, cmpfsRdd1, cnt, writer)

    val fsRdd2 = Rdd.filterStartingBefore(slicest2).filterEndingAfter(sliceet2).slice(slicest2, sliceet2).fill("linear")
    val cmpfsRdd2 = cmpRdd.filterStartingBefore(slicest2).filterEndingAfter(sliceet2).slice(slicest2, sliceet2).fill("linear")
    if (rddEquals(fsRdd2, cmpfsRdd2)) {
        writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed\n")
    }
    else {
        writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed\n")
        return
    }
    averageTime(fsRdd2, cmpfsRdd2, cnt, writer)

    val fsRdd3 = Rdd.filterStartingBefore(slicest3).filterEndingAfter(sliceet3).slice(slicest3, sliceet3).fill("linear")
    val cmpfsRdd3 = cmpRdd.filterStartingBefore(slicest3).filterEndingAfter(sliceet3).slice(slicest3, sliceet3).fill("linear")
    if (rddEquals(fsRdd3, cmpfsRdd3)) {
        writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed\n")
    }
    else {
        writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed\n")
        return
    }
    averageTime(fsRdd3, cmpfsRdd3, cnt, writer)

    writer.write("\n\n")
    writer.flush
  }
  
  def main(args: Array[String]) {
    
    val path = "/home/hadoop/RedisLabs/TEST"
    Generate(path, 8, 512, 1024, "1981-01-01", "2016-01-01")

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    
    val writer1 = new PrintWriter(new File("result_DiskBased.out"))
    val writer2 = new PrintWriter(new File("result_Serialized.out"))
    val writer3 = new PrintWriter(new File("result_Tachyon.out"))
    val jedis = new Jedis("127.0.0.1", 6379)
    (1 to 8).foreach{ i => {
      jedis.flushAll()
      Thread sleep 60000
      jedis.close
      ImportToRedisServer(path + "/TEST" + i.toString, "TEST" + i.toString, sc, ("127.0.0.1", 6379))
      TEST(sc, writer1, "disk", 4, "TEST " + i.toString, path + "/TEST" + i.toString, "TEST" + i.toString, ("127.0.0.1", 6379)) 
      TEST(sc, writer2, "SER", 4, "TEST " + i.toString, path + "/TEST" + i.toString, "TEST" + i.toString, ("127.0.0.1", 6379)) 
      TEST(sc, writer3, "Tachyon", 4, "TEST " + i.toString, path + "/TEST" + i.toString, "TEST" + i.toString, ("127.0.0.1", 6379)) 
    }}
    
    writer1.close
    writer2.close
    writer3.close
  }
}
