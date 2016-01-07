package com.redislabs.provider.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC

import com.cloudera.sparkts.{HourFrequency, BusinessDayFrequency, DayFrequency, Frequency}
import com.cloudera.sparkts.DateTimeIndex._

import java.sql.Timestamp

import com.redislabs.provider.redis._

class FilterParser(filters: Array[Filter], fieldName: Map[String, String]) {
  private val filtersByAttr: Map[String, Array[Filter]] = filters.map(f => (getAttr(f), f)).groupBy(_._1).mapValues(a => a.map(p => p._2))
  def getStartTime: String = {
    var startTime: Timestamp = new Timestamp(90, 10, 14, 0, 0, 0, 0)
    filtersByAttr.getOrElse(fieldName("timestamp"), new Array[Filter](0)).foreach({
      case GreaterThan(attr, v) => startTime = v.asInstanceOf[Timestamp]
      case GreaterThanOrEqual(attr, v) => startTime = v.asInstanceOf[Timestamp]
      case _ => {}
    })
    val st = startTime.toString
    st.substring(0, st.indexOf(' '))
  }
  def getEndTime: String = {
    var endTime: Timestamp = new Timestamp(91, 11, 3, 0, 0, 0, 0)
    filtersByAttr.getOrElse(fieldName("timestamp"), new Array[Filter](0)).foreach({
      case LessThan(attr, v) => endTime = v.asInstanceOf[Timestamp]
      case LessThanOrEqual(attr, v) => endTime = v.asInstanceOf[Timestamp]
      case _ => {}
    })
    val et = endTime.toString
    et.substring(0, et.indexOf(' '))
  }
  def getFrequency(frequency: String): Frequency = {
    frequency match {
      case "businessDay" => new BusinessDayFrequency(1)
      case "day" => new DayFrequency(1)
      case "hour" => new HourFrequency(1)
      case _ => new BusinessDayFrequency(1)
    }
  }
  def getDateTimeIndex(frequency: String) = {
    val start = getStartTime
    val end = getEndTime
    val freq = getFrequency(frequency)
    uniform(new DateTime(start, UTC), new DateTime(end, UTC), freq)
  }
  private def getAttr(f: Filter): String = {
    f match {
      case EqualTo(attribute, value) => attribute
      case GreaterThan(attribute, value) => attribute
      case GreaterThanOrEqual(attribute, value) => attribute
      case LessThan(attribute, value) => attribute
      case LessThanOrEqual(attribute, value) => attribute
      case In(attribute, values) => attribute
      case IsNull(attribute) => attribute
      case IsNotNull(attribute) => attribute
      case StringStartsWith(attribute, value) => attribute
      case StringEndsWith(attribute, value) => attribute
      case StringContains(attribute, value) => attribute
    }
  }
}

case class InstantScan(parameters: Map[String, String])
                      (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  val host: String = parameters.getOrElse("host", "127.0.0.1")
  val port: Int = parameters.getOrElse("port", "6379").toInt
  val prefix: String = parameters.getOrElse("prefix", "TASK")
  val frequency: String = parameters.getOrElse("frequency", "businessDay")

  private def getTimeSchema: StructField = {
    StructField("instant", TimestampType, nullable = true)
  }

  private def getColSchema: Array[StructField] = {
    val start = new DateTime("0000-01-03", UTC)
    val end = new DateTime("0000-01-03", UTC)
    val dtIndex = uniform(start, end, 1.businessDays)

    var rtsRdd = sqlContext.sparkContext.fromRedisKeyPattern((host, port), prefix + "_*").getRedisTimeSeriesRDD(dtIndex)
    if (parameters.get("keyPattern") != None)
      rtsRdd = rtsRdd.filterKeys(parameters("keyPattern"))
    if (parameters.get("startingBefore") != None)
      rtsRdd = rtsRdd.filterStartingBefore(new DateTime(parameters("startingBefore"), UTC))
    if (parameters.get("endingAfter") != None)
      rtsRdd = rtsRdd.filterEndingAfter(new DateTime(parameters("endingAfter"), UTC))
    return rtsRdd.toTimeSeriesRDD().keys.map(StructField(_, DoubleType, nullable = true))
  }

  val schema: StructType = StructType(getTimeSchema +: getColSchema)

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredColumnsIndex = requiredColumns.map(schema.fieldIndex(_))
    val fp = new FilterParser(filters, Map("timestamp" -> "instant"))
    val dtindex = fp.getDateTimeIndex(frequency)

    var rtsRdd = sqlContext.sparkContext.fromRedisKeyPattern((host, port), prefix + "_*").getRedisTimeSeriesRDD(dtindex)

    if (parameters.get("keyPattern") != None)
      rtsRdd = rtsRdd.filterKeys(parameters("keyPattern"))
    if (parameters.get("startingBefore") != None)
      rtsRdd = rtsRdd.filterStartingBefore(new DateTime(parameters("startingBefore"), UTC))
    if (parameters.get("endingAfter") != None)
      rtsRdd = rtsRdd.filterEndingAfter(new DateTime(parameters("endingAfter"), UTC))
    if (parameters.get("mapSeries") != None) {
      parameters("mapSeries").split(",").foreach(ms => rtsRdd = rtsRdd.mapSeries(sqlContext.getMapSeries(ms.trim)))
    }
//    rtsRdd.toTimeSeriesRDD().toInstants().map(x => new Timestamp(x._1.getMillis()) +: x._2.toArray).map{
//      candidates => requiredColumnsIndex.map(candidates(_))
//    }.map(x => Row.fromSeq(x.toSeq))
    rtsRdd.toTimeSeriesRDD().toInstants().mapPartitions { case iter =>
      iter.map(x => new Timestamp(x._1.getMillis) +: x._2.toArray).map {
        candidates => requiredColumnsIndex.map(candidates(_))
      }.map(x => Row.fromSeq(x.toSeq))
    }
//    rtsRdd.toTimeSeriesRDD().toInstants().mapPartitions { case iter =>
//        new Iterator[Row] {
//          override def hasNext: Boolean = iter.hasNext
//          override def next(): Row = {
//            val nxt = iter.next
//            val candidate = new Timestamp(nxt._1.getMillis()) +: nxt._2.toArray
//            Row.fromSeq(requiredColumnsIndex.map(candidate(_)).toSeq)
//          }
//        }
//    }
  }
}

case class ObservationScan(parameters: Map[String, String])
                          (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  val host: String = parameters.getOrElse("host", "127.0.0.1")
  val port: Int = parameters.getOrElse("port", "6379").toInt
  val prefix: String = parameters.getOrElse("prefix", "TASK")
  val frequency: String = parameters.getOrElse("frequency", "businessDay")
  val tsCol: String = parameters.getOrElse("tsCol", "timestamp")
  val keyCol: String = parameters.getOrElse("keyCol", "key")
  val valueCol: String = parameters.getOrElse("valueCol", "value")

  val schema = new StructType(Array(
    new StructField(tsCol, TimestampType),
    new StructField(keyCol, StringType),
    new StructField(valueCol, DoubleType)
  ))

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredColumnsIndex = requiredColumns.map(schema.fieldIndex(_))
    val fp = new FilterParser(filters, Map("timestamp" -> tsCol))
    val dtindex = fp.getDateTimeIndex(frequency)

    var rtsRdd = sqlContext.sparkContext.fromRedisKeyPattern((host, port), prefix + "_*").getRedisTimeSeriesRDD(dtindex)

    if (parameters.get("keyPattern") != None)
      rtsRdd = rtsRdd.filterKeys(parameters("keyPattern"))
    if (parameters.get("startingBefore") != None)
      rtsRdd = rtsRdd.filterStartingBefore(new DateTime(parameters("startingBefore"), UTC))
    if (parameters.get("endingAfter") != None)
      rtsRdd = rtsRdd.filterEndingAfter(new DateTime(parameters("endingAfter"), UTC))
    if (parameters.get("mapSeries") != None) {
      parameters("mapSeries").split(",").foreach(ms => rtsRdd = rtsRdd.mapSeries(sqlContext.getMapSeries(ms.trim)))
    }

    rtsRdd.flatMap{
      case (key, series) => {
        series.iterator.flatMap {
          case (i, value) =>
            if (value.isNaN)
              None
            else {
              val candidates = (new Timestamp(dtindex.dateTimeAtLoc(i).getMillis)) +: key +: value +: Nil
              Some(Row.fromSeq(requiredColumnsIndex.map(candidates(_)).toSeq))
            }
        }
      }
    }
  }
}

class DefaultSource extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    if (parameters.getOrElse("type", "instant") == "observation")
      ObservationScan(parameters)(sqlContext)
    else
      InstantScan(parameters)(sqlContext)
  }
}
