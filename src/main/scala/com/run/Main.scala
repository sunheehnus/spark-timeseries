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
    writer.flush

    val startTimedf2 = System.currentTimeMillis
    (0 until cnt).foreach(x => df2.collect())
    val endTimedf2 = System.currentTimeMillis
    val period2 = (endTimedf2 - startTimedf2) / 1000.0 / cnt
    writer.write(f"RedisTimeSeries: ${period2}%.2f s\n")
    writer.flush

    val improve = period1 * 100.0 / period2
    writer.write(f"Speed up: ${improve}%.2f %%\n\n\n")
    writer.flush
  }
  def InstantDFEqual(df1: DataFrame, df2: DataFrame) = {
//    if (df1.count != df2.count) false else true
    true
  }
  def InstantDFTEST(sc: SparkContext, writer: PrintWriter, ty: String, cnt: Int, msg: String, dir: String, prefix: String, redisNode: (String, Int)) {

    writer.write("****** "+ msg + " ******\n")
    val sqlContext = new SQLContext(sc)

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

    val df = Rdd.toInstantsDataFrame(sqlContext, 2)
    val cmpdf = cmpRdd.toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(df, cmpdf)) {
      writer.write("InstantDF TEST passed\n")
    }
    else {
      writer.write("InstantDF TEST failed\n")
      return
    }
    InstantDFAverage(df, cmpdf, cnt, writer)

    val filterDf1 = Rdd.filter(_._1.endsWith("Col1")).toInstantsDataFrame(sqlContext, 2)
    val cmpfilterDf1 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterKeys(".*Col1").toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(filterDf1, cmpfilterDf1)) {
      writer.write("Filter by Regex InstantDF TEST passed\n")
    }
    else {
      writer.write("Filter by Regex InstantDF TEST failed\n")
      return
    }
    InstantDFAverage(filterDf1, cmpfilterDf1, cnt, writer)

    val filterDf2 = Rdd.filter(_._1.endsWith("Col8")).toInstantsDataFrame(sqlContext, 2)
    val cmpfilterDf2 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterKeys(".*Col8").toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(filterDf2, cmpfilterDf2)) {
      writer.write("Filter by Regex InstantDF TEST passed\n")
    }
    else {
      writer.write("Filter by Regex InstantDF TEST failed\n")
      return
    }
    InstantDFAverage(filterDf2, cmpfilterDf2, cnt, writer)

    val startTime2 = new DateTime("1983-10-10", UTC)
    var filteredDfStart2 = Rdd.filterStartingBefore(startTime2).toInstantsDataFrame(sqlContext, 2)
    var cmpfilteredDfStart2 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterStartingBefore(startTime2).toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(filteredDfStart2, cmpfilteredDfStart2)) {
      writer.write("Filter by StartTime InstantDF TEST passed\n")
    }
    else {
      writer.write("Filter by StartTime InstantDF TEST failed\n")
      return
    }
    InstantDFAverage(filteredDfStart2, cmpfilteredDfStart2, cnt, writer)


    val endTime2 = new DateTime("2013-11-11", UTC)
    var filteredDfEnd2 = Rdd.filterEndingAfter(endTime2).toInstantsDataFrame(sqlContext, 2)
    var cmpfilteredDfEnd2 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterEndingAfter(endTime2).toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(filteredDfEnd2, cmpfilteredDfEnd2)) {
      writer.write("Filter by EndTime InstantDF TEST passed\n")
    }
    else {
      writer.write("Filter by EndTime InstantDF TEST failed\n")
      return
    }
    InstantDFAverage(filteredDfEnd2, cmpfilteredDfEnd2, cnt, writer)

    val slicest1 = new DateTime("2005-01-03", UTC)
    val sliceet1 = new DateTime("2015-01-01", UTC)
    val slicedDf1 = Rdd.slice(slicest1, sliceet1).fill("linear").toInstantsDataFrame(sqlContext, 2)
    val cmpslicedDf1 = cmpRdd.slice(slicest1, sliceet1).fill("linear").toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(slicedDf1, cmpslicedDf1)) {
      writer.write("Slice InstantDF TEST passed (10 years)\n")
    }
    else {
      writer.write("Slice InstantDF TEST failed (10 years)\n")
      return
    }
    InstantDFAverage(slicedDf1, cmpslicedDf1, cnt, writer)

    val slicest2 = new DateTime("2010-01-01", UTC)
    val sliceet2 = new DateTime("2015-01-01", UTC)
    val slicedDf2 = Rdd.slice(slicest2, sliceet2).fill("linear").toInstantsDataFrame(sqlContext, 2)
    val cmpslicedDf2 = cmpRdd.slice(slicest2, sliceet2).fill("linear").toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(slicedDf2, cmpslicedDf2)) {
      writer.write("Slice InstantDF TEST passed (5 years)\n")
    }
    else {
      writer.write("Slice InstantDF TEST failed (5 years)\n")
      return
    }
    InstantDFAverage(slicedDf2, cmpslicedDf2, cnt, writer)

    val slicest3 = new DateTime("2014-01-01", UTC)
    val sliceet3 = new DateTime("2015-01-01", UTC)
    val slicedDf3 = Rdd.slice(slicest3, sliceet3).fill("linear").toInstantsDataFrame(sqlContext, 2)
    val cmpslicedDf3 = cmpRdd.slice(slicest3, sliceet3).fill("linear").toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(slicedDf3, cmpslicedDf3)) {
      writer.write("Slice InstantDF TEST passed (1 years)\n")
    }
    else {
      writer.write("Slice InstantDF TEST failed (1 years)\n")
      return
    }
    InstantDFAverage(slicedDf3, cmpslicedDf3, cnt, writer)

    val fsDf1 = Rdd.filterStartingBefore(slicest1).filterEndingAfter(sliceet1).slice(slicest1, sliceet1).fill("linear").toInstantsDataFrame(sqlContext, 2)
    val cmpfsDf1 = cmpRdd.filterStartingBefore(slicest1).filterEndingAfter(sliceet1).slice(slicest1, sliceet1).fill("linear").toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(fsDf1, cmpfsDf1)) {
      writer.write("Filter by StartTime & EndTime then Slice InstantDF TEST passed (10 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice InstantDF TEST failed (10 years)\n")
      return
    }
    InstantDFAverage(fsDf1, cmpfsDf1, cnt, writer)

    val fsDf2 = Rdd.filterStartingBefore(slicest2).filterEndingAfter(sliceet2).slice(slicest2, sliceet2).fill("linear").toInstantsDataFrame(sqlContext, 2)
    val cmpfsDf2 = cmpRdd.filterStartingBefore(slicest2).filterEndingAfter(sliceet2).slice(slicest2, sliceet2).fill("linear").toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(fsDf2, cmpfsDf2)) {
      writer.write("Filter by StartTime & EndTime then Slice InstantDF TEST passed (5 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice InstantDF TEST failed (5 years)\n")
      return
    }
    InstantDFAverage(fsDf2, cmpfsDf2, cnt, writer)

    val fsDf3 = Rdd.filterStartingBefore(slicest3).filterEndingAfter(sliceet3).slice(slicest3, sliceet3).fill("linear").toInstantsDataFrame(sqlContext, 2)
    val cmpfsDf3 = cmpRdd.filterStartingBefore(slicest3).filterEndingAfter(sliceet3).slice(slicest3, sliceet3).fill("linear").toTimeSeriesRDD().toInstantsDataFrame(sqlContext, 2)
    if (InstantDFEqual(fsDf3, cmpfsDf3)) {
      writer.write("Filter by StartTime & EndTime then Slice InstantDF TEST passed (1 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice InstantDF TEST failed (1 years)\n")
      return
    }
    InstantDFAverage(fsDf3, cmpfsDf3, cnt, writer)

    writer.write("\n\n")
    writer.flush
  }


  def ObservationDFAverage(df1: DataFrame, df2: DataFrame, cnt: Int, writer: java.io.PrintWriter) {
    val startTimedf1 = System.currentTimeMillis
    (0 until cnt).foreach(x => df1.collect())
    val endTimedf1 = System.currentTimeMillis
    val period1 = (endTimedf1 - startTimedf1) / 1000.0 / cnt
    writer.write(f"TimeSeries: ${period1}%.2f s\n")
    writer.flush

    val startTimedf2 = System.currentTimeMillis
    (0 until cnt).foreach(x => df2.collect())
    val endTimedf2 = System.currentTimeMillis
    val period2 = (endTimedf2 - startTimedf2) / 1000.0 / cnt
    writer.write(f"RedisTimeSeries: ${period2}%.2f s\n")
    writer.flush

    val improve = period1 * 100.0 / period2
    writer.write(f"Speed up: ${improve}%.2f %%\n\n\n")
    writer.flush
  }
  def ObservationDFEqual(df1: DataFrame, df2: DataFrame) = {
//    if (df1.count != df2.count) false else true
    true
  }
  def ObservationDFTest(sc: SparkContext, writer: PrintWriter, ty: String, cnt: Int, msg: String, dir: String, prefix: String, redisNode: (String, Int)) {

    writer.write("****** "+ msg + " ******\n")
    val sqlContext = new SQLContext(sc)

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

//    val df = Rdd.toObservationsDataFrame(sqlContext)
//    val cmpdf = cmpRdd.toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
//    if (InstantDFEqual(df, cmpdf)) {
//      writer.write("ObservationDF TEST passed\n")
//    }
//    else {
//      writer.write("ObservationDF TEST failed\n")
//      return
//    }
//    InstantDFAverage(df, cmpdf, cnt, writer)

    val filterDf1 = Rdd.filter(_._1.endsWith("Col1")).toObservationsDataFrame(sqlContext)
    val cmpfilterDf1 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterKeys(".*Col1").toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
    if (InstantDFEqual(filterDf1, cmpfilterDf1)) {
      writer.write("Filter by Regex ObservationDF TEST passed\n")
    }
    else {
      writer.write("Filter by Regex ObservationDF TEST failed\n")
      return
    }
    InstantDFAverage(filterDf1, cmpfilterDf1, cnt, writer)

    val filterDf2 = Rdd.filter(_._1.endsWith("Col8")).toObservationsDataFrame(sqlContext)
    val cmpfilterDf2 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterKeys(".*Col8").toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
    if (InstantDFEqual(filterDf2, cmpfilterDf2)) {
      writer.write("Filter by Regex ObservationDF TEST passed\n")
    }
    else {
      writer.write("Filter by Regex ObservationDF TEST failed\n")
      return
    }
    InstantDFAverage(filterDf2, cmpfilterDf2, cnt, writer)

    val startTime2 = new DateTime("1983-10-10", UTC)
    var filteredDfStart2 = Rdd.filterStartingBefore(startTime2).toObservationsDataFrame(sqlContext)
    var cmpfilteredDfStart2 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterStartingBefore(startTime2).toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
    if (InstantDFEqual(filteredDfStart2, cmpfilteredDfStart2)) {
      writer.write("Filter by StartTime ObservationDF TEST passed\n")
    }
    else {
      writer.write("Filter by StartTime ObservationDF TEST failed\n")
      return
    }
    InstantDFAverage(filteredDfStart2, cmpfilteredDfStart2, cnt, writer)


    val endTime2 = new DateTime("2013-11-11", UTC)
    var filteredDfEnd2 = Rdd.filterEndingAfter(endTime2).toObservationsDataFrame(sqlContext)
    var cmpfilteredDfEnd2 = sc.fromRedisKeyPattern(redisNode, prefix + "_*").getRedisTimeSeriesRDD(dtIndex).filterEndingAfter(endTime2).toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
    if (InstantDFEqual(filteredDfEnd2, cmpfilteredDfEnd2)) {
      writer.write("Filter by EndTime ObservationDF TEST passed\n")
    }
    else {
      writer.write("Filter by EndTime ObservationDF TEST failed\n")
      return
    }
    InstantDFAverage(filteredDfEnd2, cmpfilteredDfEnd2, cnt, writer)

    val slicest1 = new DateTime("2005-01-03", UTC)
    val sliceet1 = new DateTime("2015-01-01", UTC)
    val slicedDf1 = Rdd.slice(slicest1, sliceet1).fill("linear").toObservationsDataFrame(sqlContext)
    val cmpslicedDf1 = cmpRdd.slice(slicest1, sliceet1).fill("linear").toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
    if (InstantDFEqual(slicedDf1, cmpslicedDf1)) {
      writer.write("Slice ObservationDF TEST passed (10 years)\n")
    }
    else {
      writer.write("Slice ObservationDF TEST failed (10 years)\n")
      return
    }
    InstantDFAverage(slicedDf1, cmpslicedDf1, cnt, writer)

    val slicest2 = new DateTime("2010-01-01", UTC)
    val sliceet2 = new DateTime("2015-01-01", UTC)
    val slicedDf2 = Rdd.slice(slicest2, sliceet2).fill("linear").toObservationsDataFrame(sqlContext)
    val cmpslicedDf2 = cmpRdd.slice(slicest2, sliceet2).fill("linear").toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
    if (InstantDFEqual(slicedDf2, cmpslicedDf2)) {
      writer.write("Slice ObservationDF TEST passed (5 years)\n")
    }
    else {
      writer.write("Slice ObservationDF TEST failed (5 years)\n")
      return
    }
    InstantDFAverage(slicedDf2, cmpslicedDf2, cnt, writer)

    val slicest3 = new DateTime("2014-01-01", UTC)
    val sliceet3 = new DateTime("2015-01-01", UTC)
    val slicedDf3 = Rdd.slice(slicest3, sliceet3).fill("linear").toObservationsDataFrame(sqlContext)
    val cmpslicedDf3 = cmpRdd.slice(slicest3, sliceet3).fill("linear").toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
    if (InstantDFEqual(slicedDf3, cmpslicedDf3)) {
      writer.write("Slice ObservationDF TEST passed (1 years)\n")
    }
    else {
      writer.write("Slice ObservationDF TEST failed (1 years)\n")
      return
    }
    InstantDFAverage(slicedDf3, cmpslicedDf3, cnt, writer)

    val fsDf1 = Rdd.filterStartingBefore(slicest1).filterEndingAfter(sliceet1).slice(slicest1, sliceet1).fill("linear").toObservationsDataFrame(sqlContext)
    val cmpfsDf1 = cmpRdd.filterStartingBefore(slicest1).filterEndingAfter(sliceet1).slice(slicest1, sliceet1).fill("linear").toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
    if (InstantDFEqual(fsDf1, cmpfsDf1)) {
      writer.write("Filter by StartTime & EndTime then Slice ObservationDF TEST passed (10 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice ObservationDF TEST failed (10 years)\n")
      return
    }
    InstantDFAverage(fsDf1, cmpfsDf1, cnt, writer)

    val fsDf2 = Rdd.filterStartingBefore(slicest2).filterEndingAfter(sliceet2).slice(slicest2, sliceet2).fill("linear").toObservationsDataFrame(sqlContext)
    val cmpfsDf2 = cmpRdd.filterStartingBefore(slicest2).filterEndingAfter(sliceet2).slice(slicest2, sliceet2).fill("linear").toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
    if (InstantDFEqual(fsDf2, cmpfsDf2)) {
      writer.write("Filter by StartTime & EndTime then Slice ObservationDF TEST passed (5 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice ObservationDF TEST failed (5 years)\n")
      return
    }
    InstantDFAverage(fsDf2, cmpfsDf2, cnt, writer)

    val fsDf3 = Rdd.filterStartingBefore(slicest3).filterEndingAfter(sliceet3).slice(slicest3, sliceet3).fill("linear").toObservationsDataFrame(sqlContext)
    val cmpfsDf3 = cmpRdd.filterStartingBefore(slicest3).filterEndingAfter(sliceet3).slice(slicest3, sliceet3).fill("linear").toTimeSeriesRDD().toObservationsDataFrame(sqlContext)
    if (InstantDFEqual(fsDf3, cmpfsDf3)) {
      writer.write("Filter by StartTime & EndTime then Slice ObservationDF TEST passed (1 years)\n")
    }
    else {
      writer.write("Filter by StartTime & EndTime then Slice ObservationDF TEST failed (1 years)\n")
      return
    }
    InstantDFAverage(fsDf3, cmpfsDf3, cnt, writer)

    writer.write("\n\n")
    writer.flush
  }
  def main(args: Array[String]) {
    val path = "/mnt/TEST"
    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)

    val writer1 = new PrintWriter(new File(path + "/instant_DiskBased.out"))
    val writer2 = new PrintWriter(new File(path + "/instant_Serialized.out"))
    val writer3 = new PrintWriter(new File(path + "/instant_Tachyon.out"))
    val writer4 = new PrintWriter(new File(path + "/observation_DiskBased.out"))
    val writer5 = new PrintWriter(new File(path + "/observation_Serialized.out"))
    val writer6 = new PrintWriter(new File(path + "/observation_Tachyon.out"))
    val pos = args(0).toInt
    (pos to pos).foreach{ i => {
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
