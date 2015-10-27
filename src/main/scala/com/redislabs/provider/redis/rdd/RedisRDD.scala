package com.redislabs.provider.redis.rdd

import java.net.InetAddress
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark._
import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._
import com.redislabs.provider.redis.partitioner._
import com.redislabs.provider.RedisConfig
import com.redislabs.provider.redis._

import com.cloudera.sparkts._
import com.cloudera.sparkts.DateTimeIndex._

import com.github.nscala_time.time.Imports._

import breeze.linalg._
import breeze.stats._

import org.joda.time.DateTimeZone.UTC

class RedisTimeSeriesRDD(prev: RDD[String],
                         index: DateTimeIndex,
                         pattern: String = null,
                         startTime: DateTime = null,
                         endTime: DateTime = null,
                         f: (Vector[Double]) => Vector[Double] = null)
    extends RDD[(String, Vector[Double])](prev) with Keys {

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(String, Vector[Double])] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val sPos = partition.slots._1
    val ePos = partition.slots._2
    val nodes = partition.redisConfig.getNodesBySlots(sPos, ePos)
    val keys = firstParent[String].iterator(split, context)
    fetchTimeSeriesData(nodes, keys)
  }

  /**
   * @param jedis
   * @param keys
   * @param startTime
   * @return keys whose start_time <= startTime
   */
  private def filterKeysByStartTime(jedis: Jedis, keys: Array[String], startTime: DateTime): Array[String] = {
    if (startTime == null)
      return keys
    val st = startTime.getMillis
    val pipeline = jedis.pipelined
    keys.foreach(x => pipeline.zrangeWithScores(x, 0, 0))
    val dts = pipeline.syncAndReturnAll.flatMap { x =>
      (x.asInstanceOf[java.util.Set[Tuple]]).map(tup => tup.getScore.toLong)
    }
    (keys).zip(dts).filter(x => (x._2 <= st)).map(x => x._1)
  }

  /**
   * @param jedis
   * @param keys
   * @param endTime
   * @return keys whose end_time >= endTime
   */
  private def filterKeysByEndTime(jedis: Jedis, keys: Array[String], endTime: DateTime): Array[String] = {
    if (endTime == null)
      return keys
    val et = endTime.getMillis
    val pipeline = jedis.pipelined
    keys.foreach(x => pipeline.zrangeWithScores(x, -1, -1))
    val dts = pipeline.syncAndReturnAll.flatMap { x =>
      (x.asInstanceOf[java.util.Set[Tuple]]).map(tup => tup.getScore.toLong)
    }
    (keys).zip(dts).filter(x => (x._2 >= et)).map(x => x._1)
  }

  def fetchTimeSeriesData(nodes: Array[(String, Int, Int, Int, Int, Int)], keys: Iterator[String]): Iterator[(String, Vector[Double])] = {
    val miArr = index.toMillisArray
    val st = miArr(0)
    val et = miArr(miArr.length - 1)
    groupKeysByNode(nodes, keys).flatMap {
      x =>
        {
          val jedis = new Jedis("/tmp/redis.sock")
          val zsetKeys = filterKeysByType(jedis, x._2, "zset")
          val startTimeKeys = filterKeysByStartTime(jedis, zsetKeys, startTime)
          val endTimeKeys = filterKeysByEndTime(jedis, startTimeKeys, endTime)
          val client = new Client("/tmp/redis.sock")
          val res = endTimeKeys.flatMap {
            x =>
              {
                val prefixStartPos = x.indexOf("_RedisTS_") + 9
                val prefixEndPos = x.indexOf("_RedisTS_", prefixStartPos)
                val prefix = x.substring(prefixStartPos, prefixEndPos)
                val cols = x.substring(prefixEndPos + 9).split(",").map(prefix + _)
                
                val keysWithCols = if (pattern != null) cols.zip(0 until cols.size).filter(x => x._1.matches(pattern)) else cols.zip(0 until cols.size)
                
                if (keysWithCols.size != 0)
                  client.zrangeByScoreWithScores(x, st, et)
                val list = if (keysWithCols.size != 0) client.getMultiBulkReply else null
                
                (0 until keysWithCols.size).map{
                  i => {
                    val key = keysWithCols(i)._1
                    val col = keysWithCols(i)._2
                    val arr = new Array[Double](index.size)
                    for (i <- 0 until index.size) {
                      arr(i) = Double.NaN
                    }
                    
                    val it = list.iterator
                    while (it.hasNext) {
                      val elem = it.next
                      val time = it.next.toLong
                      arr(index.locAtDateTime(time)) = elem.substring(elem.indexOf('_') + 1).split(",")(col).toDouble
                    }
                    if (f == null)
                      (key, new DenseVector(arr))
                    else
                      (key, f(new DenseVector(arr)))
                  }   
                }          
              }
          }
          jedis.close
          client.close
          res
        }
    }.iterator
  }
  
  //def collectAsTimeSeries()
  
  //def findSeries(key: String)
  
  //def differences(n: Int): RedisTimeSeriesRDD = {
    //mapSeries(vec => diff(vec.toDenseVector, n), index.islice(n, index.size))
  //}
  
  //def quotients(n: Int): RedisTimeSeriesRDD = {
    //mapSeries(UnivariateTimeSeries.quotients(_, n), index.islice(n, index.size))
  //}
  
  //def returnRates(): RedisTimeSeriesRDD = {
    //mapSeries(vec => UnivariateTimeSeries.price2ret(vec, 1), index.islice(1, index.size))
  //}
  def filterKeys(keyPattern: String): RedisTimeSeriesRDD = {
    new RedisTimeSeriesRDD(prev, index, keyPattern, startTime, endTime, f)
  }
  
  def filterStartingBefore(dt: DateTime): RedisTimeSeriesRDD = {
    new RedisTimeSeriesRDD(prev, index, pattern, dt, endTime, f)
  }
  def filterEndingAfter(dt: DateTime): RedisTimeSeriesRDD = {
    new RedisTimeSeriesRDD(prev, index, pattern, startTime, dt, f)
  }
  
  def slice(start: DateTime, end: DateTime): RedisTimeSeriesRDD = {
    new RedisTimeSeriesRDD(prev, index.slice(start, end), pattern, startTime, endTime, f) 
  }
  def slice(start: Long, end: Long): RedisTimeSeriesRDD = {
    slice(new DateTime(start, UTC), new DateTime(end, UTC))
  }
  
  def fill(method: String): RedisTimeSeriesRDD = {
    mapSeries(UnivariateTimeSeries.fillts(_, method))
  }
  
  def mapSeries[U](f: (Vector[Double]) => Vector[Double]): RedisTimeSeriesRDD = {
    new RedisTimeSeriesRDD(prev, index, pattern, startTime, endTime, f)
  }
  
  def mapSeries[U](f: (Vector[Double]) => Vector[Double], index: DateTimeIndex): RedisTimeSeriesRDD = {
    new RedisTimeSeriesRDD(prev, index, pattern, startTime, endTime, f)
  }
  
  //def seriesStats(): RDD[StatCounter] = {
    //map(kt => new StatCounter(kt._2.valuesIterator))
  //}
}

class RedisKeysRDD(sc: SparkContext,
                   val redisNode: (String, Int),
                   val keyPattern: String = "*",
                   val partitionNum: Int = 3)
    extends RDD[String](sc, Seq.empty) with Logging with Keys {

  override protected def getPartitions: Array[Partition] = {
    val cnt = 16384 / partitionNum
    (0 until partitionNum).map(i => {
      new RedisPartition(i,
        new RedisConfig(redisNode._1, redisNode._2),
        (if (i == 0) 0 else cnt * i + 1, if (i != partitionNum - 1) cnt * (i + 1) else 16383)).asInstanceOf[Partition]
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val sPos = partition.slots._1
    val ePos = partition.slots._2
    val nodes = partition.redisConfig.getNodesBySlots(sPos, ePos)
    getKeys(nodes, sPos, ePos, keyPattern).iterator;
  }
  
  def getRedisTimeSeriesRDD(index: DateTimeIndex) = {
    new RedisTimeSeriesRDD(this, index)
  }
}

trait Keys {
  /**
   * @param key
   * @return true if the key is a RedisRegex
   */
  private def isRedisRegex(key: String) = {
    def judge(key: String, escape: Boolean): Boolean = {
      if (key.length == 0)
        return false
      escape match {
        case true => judge(key.substring(1), false);
        case false => {
          key.charAt(0) match {
            case '*'  => true;
            case '?'  => true;
            case '['  => true;
            case '\\' => judge(key.substring(1), true);
            case _    => judge(key.substring(1), false);
          }
        }
      }
    }
    judge(key, false)
  }

  /**
   * @param jedis
   * @param params
   * @return keys of params pattern in jedis
   */
  private def scanKeys(jedis: Jedis, params: ScanParams): util.HashSet[String] = {
    val keys = new util.HashSet[String]
    var cursor = "0"
    do {
      val scan = jedis.scan(cursor, params)
      keys.addAll(scan.getResult)
      cursor = scan.getStringCursor
    } while (cursor != "0")
    keys
  }

  /**
   * @param nodes list of nodes(IP:String, port:Int, index:Int, range:Int, startSlot:Int, endSlot:Int)
   * @param sPos start position of slots
   * @param ePos end position of slots
   * @param keyPattern
   * return keys whose slot is in [sPos, ePos]
   */
  def getKeys(nodes: Array[(String, Int, Int, Int, Int, Int)], sPos: Int, ePos: Int, keyPattern: String) = {
    val keys = new util.HashSet[String]()
    if (isRedisRegex(keyPattern)) {
      nodes.foreach(node => {
        val jedis = new Jedis("/tmp/redis.sock")
        val params = new ScanParams().`match`(keyPattern)
        val res = keys.addAll(scanKeys(jedis, params).filter(key => {
          val slot = JedisClusterCRC16.getSlot(key)
          slot >= sPos && slot <= ePos
        }))
        jedis.close
        res
      })
    } else {
      val slot = JedisClusterCRC16.getSlot(keyPattern)
      if (slot >= sPos && slot <= ePos)
        keys.add(keyPattern)
    }
    keys
  }

  /**
   * @param nodes list of nodes(IP:String, port:Int, index:Int, range:Int, startSlot:Int, endSlot:Int)
   * @param keys list of keys
   * return (node: (key1, key2, ...), node2: (key3, key4,...), ...)
   */
  def groupKeysByNode(nodes: Array[(String, Int, Int, Int, Int, Int)], keys: Iterator[String]) = {
    def getNode(key: String) = {
      val slot = JedisClusterCRC16.getSlot(key)
      nodes.filter(node => { node._5 <= slot && node._6 >= slot }).filter(_._3 == 0)(0) // master only
    }
    keys.map(key => (getNode(key), key)).toArray.groupBy(_._1).map(x => (x._1, x._2.map(_._2)))
  }

  /**
   * @param jedis
   * @param keys
   * keys are guaranteed that they belongs with the server jedis connected to.
   * Filter all the keys of "t" type.
   */
  def filterKeysByType(jedis: Jedis, keys: Array[String], t: String) = {
    val pipeline = jedis.pipelined
    keys.foreach(pipeline.`type`)
    val types = pipeline.syncAndReturnAll
    (keys).zip(types).filter(x => (x._2 == t)).map(x => x._1)
  }
}
