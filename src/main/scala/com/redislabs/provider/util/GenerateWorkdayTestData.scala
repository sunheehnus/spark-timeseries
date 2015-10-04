package com.redislabs.provider.util

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTimeZone.UTC
import com.cloudera.sparkts.DateTimeIndex._
import java.io._
import java.util._
/**
 * @author sunheehnus
 */
object GenerateWorkdayTestData {
  def Generate(dir: String) {
    val rnd = new Random
    val stMillis = new DateTime("1981-01-01", UTC).getMillis
    val edMillis = new DateTime("2016-01-01", UTC).getMillis
    (1 to 16).foreach{ i=> {
      val time1 = stMillis + (rnd.nextLong.abs % (edMillis - stMillis))
      val time2 = stMillis + (rnd.nextLong.abs % (edMillis - stMillis))
      val _floor = nextBusinessDay(new DateTime(if (time1 > time2) time2 else time1, UTC)).toString
      val _ceil = nextBusinessDay(new DateTime(if (time1 > time2) time1 else time2, UTC)).toString
      val floor = _floor.toString.substring(0, _floor.toString.indexOf("T"))
      val ceil = _ceil.toString.substring(0, _ceil.toString.indexOf("T"))
      GenerateBetween(dir + "/TEST" + i.toString, floor, ceil, rnd.nextInt(16) + 1)
    }}
  }
  def GenerateBetween(dir: String, startTime: String, endTime: String, fileNum: Int) {
    val folder = new File(dir)
    folder.mkdirs()
    val rnd = new Random
    for (i <- 1 to fileNum) {
      val stMillis = new DateTime(startTime, UTC).getMillis
      val edMillis = new DateTime(endTime, UTC).getMillis
      val time1 = stMillis + (rnd.nextLong.abs % (edMillis - stMillis))
      val time2 = stMillis + (rnd.nextLong.abs % (edMillis - stMillis))
      if (time1 > time2) GenerateWorkday(dir + "/test" + i.toString + ".csv", time2, time1, rnd.nextInt(15) + 1)
      else GenerateWorkday(dir + "/test" + i.toString + ".csv", time1, time2, rnd.nextInt(15) + 1)
    }
  }
  
  def GenerateWorkday(targetName: String, startTime: Long, endTime: Long, colNum: Int) {
    val rnd = new Random
    val _floor = nextBusinessDay(new DateTime(startTime, UTC)).toString
    val _ceil = nextBusinessDay(new DateTime(endTime, UTC)).toString
    val floor = new DateTime(_floor.toString.substring(0, _floor.toString.indexOf("T")), UTC)
    val ceil = new DateTime(_ceil.toString.substring(0, _ceil.toString.indexOf("T")), UTC)
    val dates: ArrayList[DateTime] = new ArrayList[DateTime]
    var dummy_day = floor
    while (dummy_day != ceil) {
      if (rnd.nextInt(100) < 95) {
        dates.add(dummy_day)
      }
      dummy_day = nextBusinessDay(dummy_day + 1.day)
    }
    val writer = new PrintWriter(new File(targetName))
    writer.write("Date")
    for (j <- 1 to colNum) {
      writer.write(",Col" + j.toString)
    }
    writer.write("\n")
    for (i <- 0 to dates.size - 1) {
      val data = dates.get(dates.size - i - 1)
      writer.write(data.toString.substring(0, data.toString.indexOf("T")))
      for (j <- 1 to colNum) {
        val num = rnd.nextInt(10000)/100.0 + 500 * j
        writer.write(f",${num}%.2f")
      }
      writer.write("\n")
    }
    writer.close
    return
  }
}