package com.test.sql


import com.cloudera.sparkts.DayFrequency
import com.cloudera.sparkts._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import com.cloudera.sparkts.DateTimeIndex._

import java.sql.Timestamp

import breeze.linalg._

case class SCAN(parameters: Map[String, String])
                      (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  private def getTimeSchema: StructField = {
    StructField("instant", TimestampType, nullable = true)
  }

  private def getColSchema: Seq[StructField] = {
    (1 to 2048).map(_.toString).map(StructField(_, DoubleType, nullable = true))
  }

  val schema: StructType = StructType(getTimeSchema +: getColSchema)

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val rdd1 = sqlContext.sparkContext.parallelize(1 to 2048, 3)
    val rdd2 = rdd1.map(x => (x.toString, Vector((x to (x + 2048)).map(_.toDouble).toSeq:_*)))
    val start = new DateTime("2010-01-04", UTC)
    val end = start.plusDays(2048)
    val index = uniform(start, end, new DayFrequency(1))
    val rdd3 = new TimeSeriesRDD(index, rdd2)

    val requiredColumnsIndex = requiredColumns.map(schema.fieldIndex(_))

    rdd3.toInstants().mapPartitions { case iter =>
      iter.map(x => new Timestamp(x._1.getMillis) +: x._2.toArray).map {
        candidates => requiredColumnsIndex.map(candidates(_))
      }.map(x => Row.fromSeq(x.toSeq))
    }
  }
}


class DefaultSource extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    SCAN(parameters)(sqlContext)
  }
}
