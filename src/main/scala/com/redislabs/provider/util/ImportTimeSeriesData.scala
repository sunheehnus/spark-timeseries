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
  def RedisWrite(text: String, keyPrefix: String = "") = {
    val lines = text.split('\n')
    val labels = lines(0).split(',').tail.map(keyPrefix + _)
    val samples = lines.tail.map(line => {
      val tokens = line.split(',')
      val dt = new DateTime(tokens.head)
      (dt, tokens.tail.map(_.toDouble))
    })
    val mat = new DenseMatrix[Double](samples.length, samples.head._2.length)
    val dts = new Array[Long](samples.length)
    (0 until samples.length).map(i => {
      val (dt, vals) = samples(i)
      dts(i) = dt.getMillis
      mat(i to i, ::) := new DenseVector[Double](vals)
    })
    (labels, dts, mat)
  }
  def ImportToRedisServer(dir: String, prefix: String, sc: SparkContext, redisNode: (String, Int)) {
    sc.wholeTextFiles(dir).map {
      case (path, text) => RedisWrite(text, prefix + "_" + path.split('/').last)
    }.collect.foreach { x =>
      {
        val labels = x._1
        val dts = x._2
        val mat = x._3
        (0 until labels.size).foreach(i => {
          val host = getHost(labels(i), redisNode)
          setZset(host, labels(i), (for (j <- 0 to dts.length - 1) yield (j + "_" + mat(j, i), dts(j).toString)).iterator)
        })
      }
    }
  }
}