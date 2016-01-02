package com.redislabs.provider.sql

import java.sql.Timestamp

import com.cloudera.sparkts.{HourFrequency, BusinessDayFrequency, DayFrequency, Frequency}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import com.redislabs.provider.redis._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import com.cloudera.sparkts.DateTimeIndex._


class FilterParser(filters: Array[Filter]) {
  private val filtersByAttr: Map[String, Array[Filter]] = filters.map(f => (getAttr(f), f)).groupBy(_._1).mapValues(a => a.map(p => p._2))
  def getStartTime: String = {
    var startTime: Timestamp = new Timestamp(90, 10, 14, 0, 0, 0, 0)
    filtersByAttr.getOrElse("instant", new Array[Filter](0)).foreach({
      case GreaterThan(attr, v) => startTime = v.asInstanceOf[Timestamp]
      case GreaterThanOrEqual(attr, v) => startTime = v.asInstanceOf[Timestamp]
      case _ => {}
    })
    val st = startTime.toString
    st.substring(0, st.indexOf(' '))
  }
  def getEndTime: String = {
    var endTime: Timestamp = new Timestamp(91, 11, 3, 0, 0, 0, 0)
    filtersByAttr.getOrElse("instant", new Array[Filter](0)).foreach({
      case LessThan(attr, v) => endTime = v.asInstanceOf[Timestamp]
      case LessThanOrEqual(attr, v) => endTime = v.asInstanceOf[Timestamp]
      case _ => {}
    })
    val et = endTime.toString
    et.substring(0, et.indexOf(' '))
  }
  def getFrequency(frequency: String): Frequency = {
    var fq: Frequency = new BusinessDayFrequency(1)
    filtersByAttr.getOrElse("frequency", new Array[Filter](0)).foreach({
      case EqualTo(attr, v) => fq = (v.asInstanceOf[String]) match {
        case "businessDay" => new BusinessDayFrequency(1)
        case "day" => new DayFrequency(1)
        case "hour" => new HourFrequency(1)
      }
    })
    fq
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

case class InstantScan(val host: String,
                       val port: Int,
                       val prefix: String,
                       val frequency: String)
                      (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  private def getTimeSchema: StructField = {
    StructField("instant", TimestampType, nullable = true)
  }

  private def getColSchema: Array[StructField] = {
    val start = new DateTime("0000-01-03", UTC)
    val end = new DateTime("0000-01-03", UTC)
    val dtIndex = uniform(start, end, 1.businessDays)
    sqlContext.sparkContext.fromRedisKeyPattern(("127.0.0.1", 6379), "TEST1_*").
      getRedisTimeSeriesRDD(dtIndex).toTimeSeriresRDD().keys.
      map(StructField(_, DoubleType, nullable = true))
  }

  val schema: StructType = StructType(getTimeSchema +: getColSchema)

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredColumnsIndex = requiredColumns.map(schema.fieldIndex(_))
    val fp = new FilterParser(filters)
    val dtindex = fp.getDateTimeIndex(frequency)
    val cmpRdd = sqlContext.sparkContext.fromRedisKeyPattern((host, port), prefix + "_*").getRedisTimeSeriesRDD(dtindex)
    cmpRdd.toTimeSeriresRDD().toInstants().map(x => new Timestamp(x._1.getMillis()) +: x._2.toArray).map{
      candidates => requiredColumnsIndex.map(candidates(_))
    }.map(x => Row.fromSeq(x.toSeq))
  }
}

case class ObservationScan(val host: String,
                           val port: Int,
                           val prefix: String,
                           val frequency: String,
                           val tsCol: String,
                           val keyCol: String,
                           val valueCol: String)
                          (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  val schema = new StructType(Array(
    new StructField(tsCol, TimestampType),
    new StructField(keyCol, StringType),
    new StructField(valueCol, DoubleType)
  ))

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredColumnsIndex = requiredColumns.map(schema.fieldIndex(_))
    requiredColumnsIndex.foreach(println)
    val fp = new FilterParser(filters)
    val dtindex = fp.getDateTimeIndex(frequency)
    val cmpRdd = sqlContext.sparkContext.fromRedisKeyPattern((host, port), prefix + "_*").getRedisTimeSeriesRDD(dtindex)

    cmpRdd.flatMap{
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
      ObservationScan(parameters.getOrElse("host", "127.0.0.1"),
        parameters.getOrElse("port", "6379").toInt,
        parameters.getOrElse("prefix", "TASK"),
        parameters.getOrElse("frequency", "businessDay"),
        parameters.getOrElse("tsCol", "timestamp"),
        parameters.getOrElse("keyCol", "key"),
        parameters.getOrElse("valueCol", "value")) (sqlContext)
    else
      InstantScan(parameters.getOrElse("host", "127.0.0.1"),
        parameters.getOrElse("port", "6379").toInt,
        parameters.getOrElse("prefix", "TASK"),
        parameters.getOrElse("frequency", "businessDay")) (sqlContext)
  }
}
