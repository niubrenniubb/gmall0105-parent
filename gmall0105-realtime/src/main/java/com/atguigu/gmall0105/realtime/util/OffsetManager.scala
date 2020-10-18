package com.atguigu.gmall0105.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManager {

  def getOffset(topicName:String,groupId:String):Map[TopicPartition,Long]={
      val jedis:Jedis=RedisUtil.getJedisClient
      val offsetKey="offset:"+topicName+":"+groupId
      val map = jedis.hgetAll(offsetKey)
      import scala.collection.JavaConversions._
      val kafkaOffsetMap:Map[TopicPartition,Long] =map.map{case(partitionId,offset)=>
        (new TopicPartition(topicName,partitionId.toInt),offset.toLong)
      }.toMap

      kafkaOffsetMap

  }

  def saveOffset(topicName:String,groupId:String,offsetRanges:Array[OffsetRange]):Unit={
    /*val jedis:Jedis=RedisUtil.getJedisClient*/  //如果写在这里而不是下面的if里面，那么当没有数据来的时候就会不断的获取链接但不归还
    val offsetKey="offset:"+topicName+":"+groupId
    val offsetMap:java.util.Map[String,String]=new util.HashMap()
    println("即将输出偏移量")
    for(offset <- offsetRanges){
      val partition:Int=offset.partition
      val untilOffset:Long=offset.untilOffset
      offsetMap.put(partition+"",untilOffset+"")
      println("分区："+partition+":"+offset.fromOffset+"==>"+offset.untilOffset)
    }

    if(offsetMap!=null&&offsetMap.size()>0){//如果什么数据都没拉到，这里是为null的
      val jedis:Jedis=RedisUtil.getJedisClient
      jedis.hmset(offsetKey,offsetMap)
      jedis.close()
    }

  }

}
