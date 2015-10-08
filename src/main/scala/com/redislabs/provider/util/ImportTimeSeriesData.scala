package com.redislabs.provider.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import breeze.linalg._
import breeze.numerics._

import com.redislabs.provider.redis._

import redis.clients.jedis._
import redis.clients.jedis.{ HostAndPort, JedisCluster }

import com.redislabs.provider.redis.rdd._
import com.redislabs.provider.redis.SaveToRedis._
import com.redislabs.provider.redis.NodesInfo._

object ImportTimeSeriesData {
  def RedisWrite(text: String, keyPrefix: String = "", redisNode: (String, Int)) = {
    val lines = text.split('\n')
    val label = keyPrefix + lines(0).split(",", 2).tail.head
    val samples = lines.tail.map(line => {
      val tokens = line.split(",", 2)
      val dt = new DateTime(tokens.head)
      (dt, tokens.tail.head)
    })
    
    val dts = new Array[String](samples.length)
    val mat = new Array[String](samples.length)
    (0 until samples.length).map(i => {
      val (dt, vals) = samples(i)
      dts(i) = dt.getMillis.toString
      mat(i) = vals
    })
    val host = getHost(label, redisNode)
    setZset(host, label, (for (j <- 0 to dts.length - 1) yield (j + "_" + mat(j), dts(j).toString)).iterator)
  }
  def ImportToRedisServer(dir: String, prefix: String, sc: SparkContext, redisNode: (String, Int)) {
    sc.wholeTextFiles(dir).foreach {
      case (path, text) => RedisWrite(text, prefix + "_RedisTS_" + path.split('/').last + "_RedisTS_", redisNode)
    }
  }
}