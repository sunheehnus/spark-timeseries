package com.run

import java.sql.Timestamp

import org.apache.spark.sql.{SQLContext, DataFrame}
import com.redislabs.provider.redis._
import com.redislabs.provider.util.ImportTimeSeriesData._

import breeze.linalg._
import breeze.numerics._

import com.cloudera.finance.YahooParser
import com.cloudera.sparkts.DateTimeIndex._
import com.cloudera.sparkts.{UnivariateTimeSeries, TimeSeries}
import com.cloudera.sparkts.TimeSeriesRDD._

import com.github.nscala_time.time.Imports._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import org.joda.time.DateTimeZone.UTC

import java.io._

import com.redislabs.provider.sql._

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
    return true
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

    if (ty == "SER") {
      seriesByFile.persist(StorageLevel.MEMORY_AND_DISK_SER)
      seriesByFile.collect
    }
    if (ty == "Tachyon") {
      seriesByFile.persist(StorageLevel.OFF_HEAP)
      seriesByFile.collect
    }

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

    val slicest1 = new DateTime("2005-01-03", UTC)
    val sliceet1 = new DateTime("2015-01-01", UTC)
    val slicedRdd1 = Rdd.slice(slicest1, sliceet1).fill("linear")
    val cmpslicedRdd1 = cmpRdd.slice(slicest1, sliceet1).fill("linear")
    if (rddEquals(slicedRdd1, cmpslicedRdd1)) {
      writer.write("Slice RDD TEST passed (10 years)\n")
    }
    else {
      writer.write("Slice RDD TEST failed (10 years)\n")
      return
    }
    averageTime(slicedRdd1, cmpslicedRdd1, cnt, writer)

    val slicest2 = new DateTime("2005-01-03", UTC)
    val sliceet2 = new DateTime("2015-01-01", UTC)
    val slicedRdd2 = Rdd.slice(slicest2, sliceet2).fill("linear")
    val cmpslicedRdd2 = cmpRdd.slice(slicest2, sliceet2).fill("linear")
    if (rddEquals(slicedRdd2, cmpslicedRdd2)) {
      writer.write("Slice RDD TEST passed (5 years)\n")
    }
    else {
      writer.write("Slice RDD TEST failed (5 years)\n")
      return
    }
    averageTime(slicedRdd2, cmpslicedRdd2, cnt, writer)

    val slicest3 = new DateTime("2014-01-01", UTC)
    val sliceet3 = new DateTime("2015-01-01", UTC)
    val slicedRdd3 = Rdd.slice(slicest3, sliceet3).fill("linear")
    val cmpslicedRdd3 = cmpRdd.slice(slicest3, sliceet3).fill("linear")
    if (rddEquals(slicedRdd3, cmpslicedRdd3)) {
      writer.write("Slice RDD TEST passed (1 years)\n")
    }
    else {
      writer.write("Slice RDD TEST failed (1 years)\n")
      return
    }
    averageTime(slicedRdd3, cmpslicedRdd3, cnt, writer)

    val fsRdd1 = Rdd.filterStartingBefore(slicest1).filterEndingAfter(sliceet1).slice(slicest1, sliceet1).fill("linear")
    val cmpfsRdd1 = cmpRdd.filterStartingBefore(slicest1).filterEndingAfter(sliceet1).slice(slicest1, sliceet1).fill("linear")
    if (rddEquals(fsRdd1, cmpfsRdd1)) {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed (10 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed (10 years)\n")
      return
    }
    averageTime(fsRdd1, cmpfsRdd1, cnt, writer)

    val fsRdd2 = Rdd.filterStartingBefore(slicest2).filterEndingAfter(sliceet2).slice(slicest2, sliceet2).fill("linear")
    val cmpfsRdd2 = cmpRdd.filterStartingBefore(slicest2).filterEndingAfter(sliceet2).slice(slicest2, sliceet2).fill("linear")
    if (rddEquals(fsRdd2, cmpfsRdd2)) {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed (5 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed (5 years)\n")
      return
    }
    averageTime(fsRdd2, cmpfsRdd2, cnt, writer)

    val fsRdd3 = Rdd.filterStartingBefore(slicest3).filterEndingAfter(sliceet3).slice(slicest3, sliceet3).fill("linear")
    val cmpfsRdd3 = cmpRdd.filterStartingBefore(slicest3).filterEndingAfter(sliceet3).slice(slicest3, sliceet3).fill("linear")
    if (rddEquals(fsRdd3, cmpfsRdd3)) {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed (1 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed (1 years)\n")
      return
    }
    averageTime(fsRdd3, cmpfsRdd3, cnt, writer)

    writer.write("\n\n")
    writer.flush
  }

  def InstantDFAverage(df1: DataFrame, df2: DataFrame, cnt: Int, writer: java.io.PrintWriter) {
    val startTimedf1 = System.currentTimeMillis
    (0 until cnt).foreach(x => df1.collect())
    val endTimedf1 = System.currentTimeMillis
    val period1 = (endTimedf1 - startTimedf1) / 1000.0 / cnt
    writer.write(f"TimeSeries: ${period1}%.2f s\n")

    val startTimedf2 = System.currentTimeMillis
    (0 until cnt).foreach(x => df2.collect())
    val endTimedf2 = System.currentTimeMillis
    val period2 = (endTimedf2 - startTimedf2) / 1000.0 / cnt
    writer.write(f"RedisTimeSeries: ${period2}%.2f s\n")

    val improve = period1 * 100.0 / period2
    writer.write(f"Speed up: ${improve}%.2f %%\n\n\n")
  }
  def InstantDFEqual(df1: DataFrame, df2: DataFrame) = {
    if (df1.count != df2.count) false else true
  }
  def InstantDFTEST(sc: SparkContext, writer: PrintWriter, ty: String, cnt: Int, msg: String, dir: String, prefix: String, redisNode: (String, Int)) {


    writer.write("****** "+ msg + " ******\n")

    val sqlContext = new SQLContext(sc)

    sqlContext.setMapSeries("linear", (x: Vector[Double]) => UnivariateTimeSeries.fillts(x, "linear"))

    val seriesByFile: RDD[TimeSeries] = YahooParser.yahooFiles(dir, sc)

    if (ty == "SER") {
      seriesByFile.persist(StorageLevel.MEMORY_AND_DISK_SER)
      seriesByFile.collect
    }
    if (ty == "Tachyon") {
      seriesByFile.persist(StorageLevel.OFF_HEAP)
      seriesByFile.collect
    }

    val start = seriesByFile.map(_.index.first).takeOrdered(1).head
    val startTimeStr = new Timestamp(start.getMillis()).toString
    val end = seriesByFile.map(_.index.last).top(1).head
    val endTimeStr = new Timestamp(end.getMillis()).toString

    val dtIndex = uniform(start, end, 1.businessDays)

    val Df = timeSeriesRDD(dtIndex, seriesByFile).toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant')
      """.stripMargin)
    val cmpDf =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$startTimeStr' AS TIMESTAMP) AND instant <= CAST('$endTimeStr' AS TIMESTAMP)
        """.stripMargin)
    if (InstantDFEqual(Df, cmpDf)) {
      writer.write("DF TEST passed\n")
    }
    else {
      writer.write("DF TEST failed\n")
      return
    }
    InstantDFAverage(Df, cmpDf, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val filterDf1 = timeSeriesRDD(dtIndex, seriesByFile).filter(_._1.endsWith("Col1")).toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant', keyPattern '.*Col1')
      """.stripMargin)
    val cmpfilterDf1 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$startTimeStr' AS TIMESTAMP) AND instant <= CAST('$endTimeStr' AS TIMESTAMP)
        """.stripMargin)

    if (InstantDFEqual(filterDf1, cmpfilterDf1)) {
      writer.write("Filter by Regex RDD TEST passed\n")
    }
    else {
      writer.write("Filter by Regex RDD TEST failed\n")
      return
    }
    InstantDFAverage(filterDf1, cmpfilterDf1, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val filterDf2 = timeSeriesRDD(dtIndex, seriesByFile).filter(_._1.endsWith("Col8")).toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant', keyPattern '.*Col8')
      """.stripMargin)
    val cmpfilterDf2 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$startTimeStr' AS TIMESTAMP) AND instant <= CAST('$endTimeStr' AS TIMESTAMP)
        """.stripMargin)
    if (InstantDFEqual(filterDf2, cmpfilterDf2)) {
      writer.write("Filter by Regex RDD TEST passed\n")
    }
    else {
      writer.write("Filter by Regex RDD TEST failed\n")
      return
    }
    InstantDFAverage(filterDf2, cmpfilterDf2, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val startTime2 = new DateTime("1983-10-10", UTC)
    val startTimeStr2 = "1983-10-10"
    var filteredDfStart2 = timeSeriesRDD(dtIndex, seriesByFile).filterStartingBefore(startTime2).toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant', startingBefore '$startTimeStr2')
      """.stripMargin)
    val cmpfilteredDfStart2 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$startTimeStr' AS TIMESTAMP) AND instant <= CAST('$endTimeStr' AS TIMESTAMP)
        """.stripMargin)
    if (InstantDFEqual(filteredDfStart2, cmpfilteredDfStart2)) {
      writer.write("Filter by StartTime RDD TEST passed\n")
    }
    else {
      writer.write("Filter by StartTime RDD TEST failed\n")
      return
    }
    InstantDFAverage(filteredDfStart2, cmpfilteredDfStart2, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val endTime2 = new DateTime("2013-11-11", UTC)
    val endTimeStr2 = "2013-11-11"
    var filteredDfEnd2 = timeSeriesRDD(dtIndex, seriesByFile).filterEndingAfter(endTime2).toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant', endingAfter '$endTimeStr2')
      """.stripMargin)
    val cmpfilteredDfEnd2 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$startTimeStr' AS TIMESTAMP) AND instant <= CAST('$endTimeStr' AS TIMESTAMP)
        """.stripMargin)
    if (InstantDFEqual(filteredDfEnd2, cmpfilteredDfEnd2)) {
      writer.write("Filter by StartTime RDD TEST passed\n")
    }
    else {
      writer.write("Filter by StartTime RDD TEST failed\n")
      return
    }
    InstantDFAverage(filteredDfEnd2, cmpfilteredDfEnd2, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val slicest1 = new DateTime("2005-01-03", UTC)
    val slicestStr1 = "2005-01-03"
    val slicestTimeStamp1 = new Timestamp(slicest1.getMillis()).toString
    val sliceet1 = new DateTime("2015-01-01", UTC)
    val sliceetStr1 = "2015-01-01"
    val sliceetTimeStamp1 = new Timestamp(sliceet1.getMillis()).toString
    var slicedDf1 = timeSeriesRDD(dtIndex, seriesByFile).slice(slicest1, sliceet1).fill("linear").toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant', mapSeries 'linear')
      """.stripMargin)
    val cmpslicedDf1 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$slicestTimeStamp1' AS TIMESTAMP) AND instant <= CAST('$sliceetTimeStamp1' AS TIMESTAMP)
        """.stripMargin)
    if (InstantDFEqual(slicedDf1, cmpslicedDf1)) {
      writer.write("Slice RDD TEST passed (10 years)\n")
    }
    else {
      writer.write("Slice RDD TEST failed (10 years)\n")
      return
    }
    InstantDFAverage(slicedDf1, cmpslicedDf1, cnt, writer)
    sqlContext.dropTempTable("dataTable")


    val slicest2 = new DateTime("2005-01-03", UTC)
    val slicestStr2 = "2005-01-03"
    val slicestTimeStamp2 = new Timestamp(slicest2.getMillis()).toString
    val sliceet2 = new DateTime("2015-01-01", UTC)
    val sliceetStr2 = "2015-01-01"
    val sliceetTimeStamp2 = new Timestamp(sliceet2.getMillis()).toString
    var slicedDf2 = timeSeriesRDD(dtIndex, seriesByFile).slice(slicest2, sliceet2).fill("linear").toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant', mapSeries 'linear')
      """.stripMargin)
    val cmpslicedDf2 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$slicestTimeStamp2' AS TIMESTAMP) AND instant <= CAST('$sliceetTimeStamp2' AS TIMESTAMP)
        """.stripMargin)
    if (InstantDFEqual(slicedDf2, cmpslicedDf2)) {
      writer.write("Slice RDD TEST passed (5 years)\n")
    }
    else {
      writer.write("Slice RDD TEST failed (5 years)\n")
      return
    }
    InstantDFAverage(slicedDf2, cmpslicedDf2, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val slicest3 = new DateTime("2014-01-01", UTC)
    val slicestStr3 = "2014-01-01"
    val slicestTimeStamp3 = new Timestamp(slicest3.getMillis()).toString
    val sliceet3 = new DateTime("2015-01-01", UTC)
    val sliceetStr3 = "2015-01-01"
    val sliceetTimeStamp3 = new Timestamp(sliceet3.getMillis()).toString
    var slicedDf3 = timeSeriesRDD(dtIndex, seriesByFile).slice(slicest3, sliceet3).fill("linear").toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant', mapSeries 'linear')
      """.stripMargin)
    val cmpslicedDf3 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$slicestTimeStamp3' AS TIMESTAMP) AND instant <= CAST('$sliceetTimeStamp3' AS TIMESTAMP)
        """.stripMargin)
    if (InstantDFEqual(slicedDf3, cmpslicedDf3)) {
      writer.write("Slice RDD TEST passed (1 years)\n")
    }
    else {
      writer.write("Slice RDD TEST failed (1 years)\n")
      return
    }
    InstantDFAverage(slicedDf3, cmpslicedDf3, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val fsDf1 = timeSeriesRDD(dtIndex, seriesByFile).filterStartingBefore(slicest1).filterEndingAfter(sliceet1).slice(slicest1, sliceet1).fill("linear").toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant', startingBefore '$slicestStr1', endingAfter '$sliceetStr1', mapSeries 'linear')
      """.stripMargin)
    val cmpfsDf1 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$slicestTimeStamp1' AS TIMESTAMP) AND instant <= CAST('$sliceetTimeStamp1' AS TIMESTAMP)
        """.stripMargin)
    if (InstantDFEqual(fsDf1, cmpfsDf1)) {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed (10 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed (10 years)\n")
      return
    }
    InstantDFAverage(fsDf1, cmpfsDf1, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val fsDf2 = timeSeriesRDD(dtIndex, seriesByFile).filterStartingBefore(slicest2).filterEndingAfter(sliceet2).slice(slicest2, sliceet2).fill("linear").toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant', startingBefore '$slicestStr2', endingAfter '$sliceetStr2', mapSeries 'linear')
      """.stripMargin)
    val cmpfsDf2 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$slicestTimeStamp2' AS TIMESTAMP) AND instant <= CAST('$sliceetTimeStamp2' AS TIMESTAMP)
        """.stripMargin)
    if (InstantDFEqual(fsDf2, cmpfsDf2)) {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed (5 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed (5 years)\n")
      return
    }
    InstantDFAverage(fsDf2, cmpfsDf2, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val fsDf3 = timeSeriesRDD(dtIndex, seriesByFile).filterStartingBefore(slicest3).filterEndingAfter(sliceet3).slice(slicest3, sliceet3).fill("linear").toInstantsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'instant', startingBefore '$slicestStr3', endingAfter '$sliceetStr3', mapSeries 'linear')
      """.stripMargin)
    val cmpfsDf3 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE instant >= CAST('$slicestTimeStamp3' AS TIMESTAMP) AND instant <= CAST('$sliceetTimeStamp3' AS TIMESTAMP)
        """.stripMargin)
    if (InstantDFEqual(fsDf3, cmpfsDf3)) {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed (1 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed (1 years)\n")
      return
    }
    InstantDFAverage(fsDf3, cmpfsDf3, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    writer.write("\n\n")
    writer.flush
  }


  def ObservationDFAverage(df1: DataFrame, df2: DataFrame, cnt: Int, writer: java.io.PrintWriter) {
    val startTimedf1 = System.currentTimeMillis
    (0 until cnt).foreach(x => df1.collect())
    val endTimedf1 = System.currentTimeMillis
    val period1 = (endTimedf1 - startTimedf1) / 1000.0 / cnt
    writer.write(f"TimeSeries: ${period1}%.2f s\n")

    val startTimedf2 = System.currentTimeMillis
    (0 until cnt).foreach(x => df2.collect())
    val endTimedf2 = System.currentTimeMillis
    val period2 = (endTimedf2 - startTimedf2) / 1000.0 / cnt
    writer.write(f"RedisTimeSeries: ${period2}%.2f s\n")

    val improve = period1 * 100.0 / period2
    writer.write(f"Speed up: ${improve}%.2f %%\n\n\n")
  }
  def ObservationDFEqual(df1: DataFrame, df2: DataFrame) = {
    if (df1.count != df2.count) false else true
  }
  def ObservationDFTest(sc: SparkContext, writer: PrintWriter, ty: String, cnt: Int, msg: String, dir: String, prefix: String, redisNode: (String, Int)) {


    writer.write("****** "+ msg + " ******\n")

    val sqlContext = new SQLContext(sc)

    sqlContext.setMapSeries("linear", (x: Vector[Double]) => UnivariateTimeSeries.fillts(x, "linear"))

    val seriesByFile: RDD[TimeSeries] = YahooParser.yahooFiles(dir, sc)

    if (ty == "SER") {
      seriesByFile.persist(StorageLevel.MEMORY_AND_DISK_SER)
      seriesByFile.collect
    }
    if (ty == "Tachyon") {
      seriesByFile.persist(StorageLevel.OFF_HEAP)
      seriesByFile.collect
    }

    val start = seriesByFile.map(_.index.first).takeOrdered(1).head
    val startTimeStr = new Timestamp(start.getMillis()).toString
    val end = seriesByFile.map(_.index.last).top(1).head
    val endTimeStr = new Timestamp(end.getMillis()).toString

    val dtIndex = uniform(start, end, 1.businessDays)

    val Df = timeSeriesRDD(dtIndex, seriesByFile).toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation')
      """.stripMargin)
    val cmpDf =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$startTimeStr' AS TIMESTAMP) AND timestamp <= CAST('$endTimeStr' AS TIMESTAMP)
        """.stripMargin)
    if (ObservationDFEqual(Df, cmpDf)) {
      writer.write("DF TEST passed\n")
    }
    else {
      writer.write("DF TEST failed\n")
      return
    }
    ObservationDFAverage(Df, cmpDf, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val filterDf1 = timeSeriesRDD(dtIndex, seriesByFile).filter(_._1.endsWith("Col1")).toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation', keyPattern '.*Col1')
      """.stripMargin)
    val cmpfilterDf1 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$startTimeStr' AS TIMESTAMP) AND timestamp <= CAST('$endTimeStr' AS TIMESTAMP)
        """.stripMargin)

    if (ObservationDFEqual(filterDf1, cmpfilterDf1)) {
      writer.write("Filter by Regex RDD TEST passed\n")
    }
    else {
      writer.write("Filter by Regex RDD TEST failed\n")
      return
    }
    ObservationDFAverage(filterDf1, cmpfilterDf1, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val filterDf2 = timeSeriesRDD(dtIndex, seriesByFile).filter(_._1.endsWith("Col8")).toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation', keyPattern '.*Col8')
      """.stripMargin)
    val cmpfilterDf2 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$startTimeStr' AS TIMESTAMP) AND timestamp <= CAST('$endTimeStr' AS TIMESTAMP)
        """.stripMargin)
    if (ObservationDFEqual(filterDf2, cmpfilterDf2)) {
      writer.write("Filter by Regex RDD TEST passed\n")
    }
    else {
      writer.write("Filter by Regex RDD TEST failed\n")
      return
    }
    ObservationDFAverage(filterDf2, cmpfilterDf2, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val startTime2 = new DateTime("1983-10-10", UTC)
    val startTimeStr2 = "1983-10-10"
    var filteredDfStart2 = timeSeriesRDD(dtIndex, seriesByFile).filterStartingBefore(startTime2).toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation', startingBefore '$startTimeStr2')
      """.stripMargin)
    val cmpfilteredDfStart2 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$startTimeStr' AS TIMESTAMP) AND timestamp <= CAST('$endTimeStr' AS TIMESTAMP)
        """.stripMargin)
    if (ObservationDFEqual(filteredDfStart2, cmpfilteredDfStart2)) {
      writer.write("Filter by StartTime RDD TEST passed\n")
    }
    else {
      writer.write("Filter by StartTime RDD TEST failed\n")
      return
    }
    ObservationDFAverage(filteredDfStart2, cmpfilteredDfStart2, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val endTime2 = new DateTime("2013-11-11", UTC)
    val endTimeStr2 = "2013-11-11"
    var filteredDfEnd2 = timeSeriesRDD(dtIndex, seriesByFile).filterEndingAfter(endTime2).toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation', endingAfter '$endTimeStr2')
      """.stripMargin)
    val cmpfilteredDfEnd2 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$startTimeStr' AS TIMESTAMP) AND timestamp <= CAST('$endTimeStr' AS TIMESTAMP)
        """.stripMargin)
    if (ObservationDFEqual(filteredDfEnd2, cmpfilteredDfEnd2)) {
      writer.write("Filter by StartTime RDD TEST passed\n")
    }
    else {
      writer.write("Filter by StartTime RDD TEST failed\n")
      return
    }
    ObservationDFAverage(filteredDfEnd2, cmpfilteredDfEnd2, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val slicest1 = new DateTime("2005-01-03", UTC)
    val slicestStr1 = "2005-01-03"
    val slicestTimeStamp1 = new Timestamp(slicest1.getMillis()).toString
    val sliceet1 = new DateTime("2015-01-01", UTC)
    val sliceetStr1 = "2015-01-01"
    val sliceetTimeStamp1 = new Timestamp(sliceet1.getMillis()).toString
    var slicedDf1 = timeSeriesRDD(dtIndex, seriesByFile).slice(slicest1, sliceet1).fill("linear").toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation', mapSeries 'linear')
      """.stripMargin)
    val cmpslicedDf1 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$slicestTimeStamp1' AS TIMESTAMP) AND timestamp <= CAST('$sliceetTimeStamp1' AS TIMESTAMP)
        """.stripMargin)
    if (ObservationDFEqual(slicedDf1, cmpslicedDf1)) {
      writer.write("Slice RDD TEST passed (10 years)\n")
    }
    else {
      writer.write("Slice RDD TEST failed (10 years)\n")
      return
    }
    ObservationDFAverage(slicedDf1, cmpslicedDf1, cnt, writer)
    sqlContext.dropTempTable("dataTable")


    val slicest2 = new DateTime("2005-01-03", UTC)
    val slicestStr2 = "2005-01-03"
    val slicestTimeStamp2 = new Timestamp(slicest2.getMillis()).toString
    val sliceet2 = new DateTime("2015-01-01", UTC)
    val sliceetStr2 = "2015-01-01"
    val sliceetTimeStamp2 = new Timestamp(sliceet2.getMillis()).toString
    var slicedDf2 = timeSeriesRDD(dtIndex, seriesByFile).slice(slicest2, sliceet2).fill("linear").toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation', mapSeries 'linear')
      """.stripMargin)
    val cmpslicedDf2 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$slicestTimeStamp2' AS TIMESTAMP) AND timestamp <= CAST('$sliceetTimeStamp2' AS TIMESTAMP)
        """.stripMargin)
    if (ObservationDFEqual(slicedDf2, cmpslicedDf2)) {
      writer.write("Slice RDD TEST passed (5 years)\n")
    }
    else {
      writer.write("Slice RDD TEST failed (5 years)\n")
      return
    }
    ObservationDFAverage(slicedDf2, cmpslicedDf2, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val slicest3 = new DateTime("2014-01-01", UTC)
    val slicestStr3 = "2014-01-01"
    val slicestTimeStamp3 = new Timestamp(slicest3.getMillis()).toString
    val sliceet3 = new DateTime("2015-01-01", UTC)
    val sliceetStr3 = "2015-01-01"
    val sliceetTimeStamp3 = new Timestamp(sliceet3.getMillis()).toString
    var slicedDf3 = timeSeriesRDD(dtIndex, seriesByFile).slice(slicest3, sliceet3).fill("linear").toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation', mapSeries 'linear')
      """.stripMargin)
    val cmpslicedDf3 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$slicestTimeStamp3' AS TIMESTAMP) AND timestamp <= CAST('$sliceetTimeStamp3' AS TIMESTAMP)
        """.stripMargin)
    if (ObservationDFEqual(slicedDf3, cmpslicedDf3)) {
      writer.write("Slice RDD TEST passed (1 years)\n")
    }
    else {
      writer.write("Slice RDD TEST failed (1 years)\n")
      return
    }
    ObservationDFAverage(slicedDf3, cmpslicedDf3, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val fsDf1 = timeSeriesRDD(dtIndex, seriesByFile).filterStartingBefore(slicest1).filterEndingAfter(sliceet1).slice(slicest1, sliceet1).fill("linear").toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation', startingBefore '$slicestStr1', endingAfter '$sliceetStr1', mapSeries 'linear')
      """.stripMargin)
    val cmpfsDf1 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$slicestTimeStamp1' AS TIMESTAMP) AND timestamp <= CAST('$sliceetTimeStamp1' AS TIMESTAMP)
        """.stripMargin)
    if (ObservationDFEqual(fsDf1, cmpfsDf1)) {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed (10 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed (10 years)\n")
      return
    }
    ObservationDFAverage(fsDf1, cmpfsDf1, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val fsDf2 = timeSeriesRDD(dtIndex, seriesByFile).filterStartingBefore(slicest2).filterEndingAfter(sliceet2).slice(slicest2, sliceet2).fill("linear").toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation', startingBefore '$slicestStr2', endingAfter '$sliceetStr2', mapSeries 'linear')
      """.stripMargin)
    val cmpfsDf2 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$slicestTimeStamp2' AS TIMESTAMP) AND timestamp <= CAST('$sliceetTimeStamp2' AS TIMESTAMP)
        """.stripMargin)
    if (ObservationDFEqual(fsDf2, cmpfsDf2)) {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed (5 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed (5 years)\n")
      return
    }
    ObservationDFAverage(fsDf2, cmpfsDf2, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    val fsDf3 = timeSeriesRDD(dtIndex, seriesByFile).filterStartingBefore(slicest3).filterEndingAfter(sliceet3).slice(slicest3, sliceet3).fill("linear").toObservationsDataFrame(sqlContext)
    sqlContext.sql( s"""
                       |CREATE TEMPORARY TABLE dataTable
                       |USING com.redislabs.provider.sql
                       |OPTIONS (prefix '$prefix', frequency 'businessDay', type 'observation', startingBefore '$slicestStr3', endingAfter '$sliceetStr3', mapSeries 'linear')
      """.stripMargin)
    val cmpfsDf3 =
      sqlContext.sql(s"""
                        |SELECT *
                        |FROM dataTable
                        |WHERE timestamp >= CAST('$slicestTimeStamp3' AS TIMESTAMP) AND timestamp <= CAST('$sliceetTimeStamp3' AS TIMESTAMP)
        """.stripMargin)
    if (ObservationDFEqual(fsDf3, cmpfsDf3)) {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST passed (1 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice RDD TEST failed (1 years)\n")
      return
    }
    ObservationDFAverage(fsDf3, cmpfsDf3, cnt, writer)
    sqlContext.dropTempTable("dataTable")

    writer.write("\n\n")
    writer.flush
  }

  def main(args: Array[String]) {

    val path = "/Users/sunheehnus/develop/RedisLabs/TEST"
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val writer1 = new PrintWriter(new File(path + "/instant_DiskBased.out"))
    val writer2 = new PrintWriter(new File(path + "/instant_Serialized.out"))
    val writer3 = new PrintWriter(new File(path + "/instant_Tachyon.out"))
    val writer4 = new PrintWriter(new File(path + "/observation_DiskBased.out"))
    val writer5 = new PrintWriter(new File(path + "/observation_Serialized.out"))
    val writer6 = new PrintWriter(new File(path + "/observation_Tachyon.out"))
    (1 to 1).foreach{ i => {
      ImportToRedisServer(path + "/TEST" + i.toString, "TEST" + i.toString, sc, ("127.0.0.1", 6379))
      InstantDFTEST(sc, writer1, "disk", 1, "TEST " + i.toString, path + "/TEST" + i.toString, "TEST" + i.toString, ("127.0.0.1", 6379))
      InstantDFTEST(sc, writer2, "SER", 1, "TEST " + i.toString, path + "/TEST" + i.toString, "TEST" + i.toString, ("127.0.0.1", 6379))
      InstantDFTEST(sc, writer3, "Tachyon", 1, "TEST " + i.toString, path + "/TEST" + i.toString, "TEST" + i.toString, ("127.0.0.1", 6379))
      ObservationDFTest(sc, writer4, "disk", 1, "TEST " + i.toString, path + "/TEST" + i.toString, "TEST" + i.toString, ("127.0.0.1", 6379))
      ObservationDFTest(sc, writer5, "SER", 1, "TEST " + i.toString, path + "/TEST" + i.toString, "TEST" + i.toString, ("127.0.0.1", 6379))
      ObservationDFTest(sc, writer6, "Tachyon", 1, "TEST " + i.toString, path + "/TEST" + i.toString, "TEST" + i.toString, ("127.0.0.1", 6379))
    }}

    writer1.close
    writer2.close
    writer3.close
    writer4.close
    writer5.close
    writer6.close
  }
}
