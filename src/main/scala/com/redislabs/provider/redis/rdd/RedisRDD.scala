package com.redislabs.provider.redis.rdd

import java.io.{File, PrintWriter}
import java.net.NetworkInterface
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark._

import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._

import com.redislabs.provider.redis.partitioner._
import com.redislabs.provider.RedisConfig

import com.cloudera.sparkts._
import com.github.nscala_time.time.Imports._
import breeze.linalg._

import org.joda.time.DateTimeZone.UTC

class RedisTimeSeriesRDD(prev: RDD[String],
                         index: DateTimeIndex,
                         pattern: String = null,
                         startTime: DateTime = null,
                         endTime: DateTime = null,
                         f: (Vector[Double]) => Vector[Double] = null)
    extends RDD[(String, Vector[Double])](prev) with Keys {

  def toTimeSeriesRDD(): TimeSeriesRDD = {
    new TimeSeriesRDD(index, this)
  }
  override def getPartitions: Array[Partition] = prev.partitions

  var idx = -1

  override def compute(split: Partition, context: TaskContext): Iterator[(String, Vector[Double])] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    idx = partition.index
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

  private def filterKeysByPattern(keys: Array[String], pattern: String) = {
    keys.filter{
      key => {
        val prefixStartPos = key.indexOf("_RedisTS_") + 9
        val prefixEndPos = key.indexOf("_RedisTS_", prefixStartPos)
        if (prefixEndPos == -1)
          false
        else {
          val prefix = key.substring(prefixStartPos, prefixEndPos)
          if (pattern == null)
            true
          else
            key.substring(prefixEndPos + 9).split(",").map(prefix + _).exists(_.matches(pattern))
        }
      }
    }
  }

  def fetchTimeSeriesData(nodes: Array[(String, Int, Int, Int, Int, Int)], keys: Iterator[String]): Iterator[(String, Vector[Double])] = {
    val st = index.first.getMillis
    val et = index.last.getMillis

    var timeType: Long = 0
    var timeStartTime: Long = 0
    var timeEndTime: Long = 0
    var timeFetch: Long = 0
    var timeBuild: Long = 0
    var sTime: Long = 0
    var eTime: Long = 0
    var msg: String = null

    val ips = NetworkInterface.getNetworkInterfaces.flatMap(_.getInetAddresses.map(_.getHostAddress)).toSet
    val res = groupKeysByNode(nodes, keys).flatMap {
      x =>
        {
          val (ip, port) = (x._1._1, x._1._2)
          val udsAddr = s"/tmp/redis_${port}.sock"
          val jedis = if (false && ips.contains(ip) && new java.io.File(udsAddr).exists) {
            msg = "UDS"
            new Jedis(udsAddr)
          } else {
            msg = "IP:PORT"
            new Jedis(ip, port)
          }
          sTime = System.currentTimeMillis
          val patternKeys = filterKeysByPattern(x._2, pattern)
          eTime = System.currentTimeMillis
          timeBuild += eTime - sTime

          sTime = System.currentTimeMillis
          val zsetKeys = filterKeysByType(jedis, patternKeys, "zset")
          eTime = System.currentTimeMillis
          timeType += eTime - sTime

          sTime = System.currentTimeMillis
          val startTimeKeys = filterKeysByStartTime(jedis, zsetKeys, startTime)
          eTime = System.currentTimeMillis
          timeStartTime += eTime - sTime

          sTime = System.currentTimeMillis
          val endTimeKeys = filterKeysByEndTime(jedis, startTimeKeys, endTime)
          eTime = System.currentTimeMillis
          timeEndTime += eTime - sTime

          if (endTimeKeys.size == 0) {
            jedis.close
            Iterator()
          }
          else {
            sTime = System.currentTimeMillis
            val client = jedis.getClient
            endTimeKeys.foreach(client.zrangeByScoreWithScores(_, st, et))

            val results = client.getMany(endTimeKeys.length).map(BuilderFactory.STRING_LIST.build(_)).iterator
            eTime = System.currentTimeMillis
            timeFetch += eTime - sTime

            sTime = System.currentTimeMillis
            val res = endTimeKeys.flatMap {
              x =>
              {
                val prefixStartPos = x.indexOf("_RedisTS_") + 9
                val prefixEndPos = x.indexOf("_RedisTS_", prefixStartPos)
                val prefix = x.substring(prefixStartPos, prefixEndPos)
                val cols = x.substring(prefixEndPos + 9).split(",").map(prefix + _)

                val keysWithCols = if (pattern != null) cols.zipWithIndex.filter(x => x._1.matches(pattern)) else cols.zipWithIndex
                val (selectedKeys, selectedCols) = keysWithCols.unzip

                val arrays = Array.ofDim[Double](keysWithCols.size, index.size)
                for (array <- arrays) java.util.Arrays.fill(array, Double.NaN)

                val it = results.next.iterator
                while (it.hasNext) {
                  val elem = it.next
                  val pos = index.locAtDateTime(it.next.toLong)
                  val values = elem.substring(elem.indexOf('_') + 1).split(",")
                  for (i <- 0 until selectedCols.size) arrays(i)(pos) = values(selectedCols(i)).toDouble
                }
                if (f == null) {
                  for (i <- 0 until selectedCols.size) yield (selectedKeys(i), new DenseVector(arrays(i)))
                }
                else {
                  for (i <- 0 until selectedCols.size) yield (selectedKeys(i), f(new DenseVector(arrays(i))))
                }
              }
            }
            jedis.close
            eTime = System.currentTimeMillis
            timeBuild += eTime - sTime
            res
          }
        }
    }.iterator
    val writer1 = new PrintWriter(new File("/tmp/filterType_partition#" + idx))
    writer1.write(msg)
    writer1.write(f"filterType_partition#${idx}%d: ${timeType}%d ms\n")
    writer1.write(f"filterStartTime_partition#${idx}%d: ${timeStartTime}%d ms\n")
    writer1.write(f"filterEndTime_partition#${idx}%d: ${timeEndTime}%d ms\n")
    writer1.write(f"Fetch_partition#${idx}%d: ${timeFetch}%d ms\n")
    writer1.write(f"Build_partition#${idx}%d: ${timeBuild}%d ms\n")
    writer1.close
    res
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


  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].redisConfig.ip)
  }

  private def scaleHostsWithPartitionNum(): Seq[(String, Int, Int, Int)] = {
    def split(host: (String, Int, Int, Int), cnt: Int) = {
      val start = host._3
      val end = host._4
      val range = (end - start) / cnt
      (0 until cnt).map(i => {
        (host._1,
          host._2,
          if (i == 0) start else (start + range * i + 1),
          if (i != cnt - 1) (start + range * (i + 1)) else end)
      })
    }

    val hosts = com.redislabs.provider.redis.NodesInfo.getHosts(redisNode)
    if (hosts.size == partitionNum)
      hosts
    else if (hosts.size < partitionNum) {
      val presExtCnt = partitionNum / hosts.size
      val lastExtCnt = if (presExtCnt * hosts.size < partitionNum) (presExtCnt + 1) else presExtCnt
      hosts.zipWithIndex.flatMap{
        case(host, idx) => {
          split(host, if (idx == hosts.size - 1) lastExtCnt else presExtCnt)
        }
      }
    }
    else {
      val presExtCnt = hosts.size / partitionNum
      val lastExtCnt = if (presExtCnt * partitionNum < hosts.size) (presExtCnt + 1) else presExtCnt
      (0 until partitionNum).map{
        idx => {
          val ip = hosts(idx * presExtCnt)._1
          val port = hosts(idx * presExtCnt)._2
          val start = hosts(idx * presExtCnt)._3
          val end = hosts(if (idx == partitionNum - 1) (hosts.size-1) else ((idx + 1) * presExtCnt - 1))._4
          (ip, port, start, end)
        }
      }
    }
  }
  override protected def getPartitions: Array[Partition] = {
    val hosts = scaleHostsWithPartitionNum()
    (0 until partitionNum).map(i => {
      new RedisPartition(i,
        new RedisConfig(hosts(i)._1, hosts(i)._2),
        (hosts(i)._3, hosts(i)._4)).asInstanceOf[Partition]
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
      cursor = scan.getCursor
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
        val jedis = new Jedis(node._1, node._2)
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
