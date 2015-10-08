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
  def Generate(dir: String, testCnt: Int, testFileFloor: Int, testFileCeil: Int, startTime: String, endTime: String) {
    val rnd = new Random
    (1 to testCnt).foreach{ i=> {
      GenerateBetween(dir + "/TEST" + i.toString, startTime, endTime, testFileFloor + rnd.nextInt(testFileCeil - testFileFloor) + 1)
    }}
  }
  def GenerateBetween(dir: String, startTime: String, endTime: String, fileNum: Int) {
    val folder = new File(dir)
    folder.mkdirs()
    
    val rnd = new Random
    for (i <- 1 to fileNum) {
      val stMillis = new DateTime(startTime, UTC).getMillis
      val edMillis = new DateTime(endTime, UTC).getMillis
      
      var floor: String = ""
      var ceil: String = ""
      do {
        val time1 = stMillis + (rnd.nextLong.abs % (edMillis - stMillis))
        val time2 = stMillis + (rnd.nextLong.abs % (edMillis - stMillis))
        floor = nextBusinessDay(new DateTime(if (time1 > time2) time2 else time1, UTC)).toString
        floor = floor.substring(0, floor.indexOf("T"))
        ceil = nextBusinessDay(new DateTime(if (time1 > time2) time1 else time2, UTC)).toString
        ceil = ceil.substring(0, ceil.indexOf("T"))
      } while (floor == ceil)
      GenerateWorkday(dir + "/test" + i.toString + ".csv", floor, ceil, rnd.nextInt(15) + 1)
    }
  }
  
  def GenerateWorkday(targetName: String, startTime: String, endTime: String, colNum: Int) {
    val rnd = new Random
    val floor = new DateTime(startTime, UTC)
    val ceil = new DateTime(endTime, UTC)
    val dates: ArrayList[DateTime] = new ArrayList[DateTime]
    var dummy_day = floor
    while (dummy_day != ceil) {
      if (rnd.nextInt(100) < 90) {
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