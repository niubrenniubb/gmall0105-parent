package com.atguigu.gmall0105.realtime

import java.sql.Date
import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.bean.DauInfo
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {

  def main(args: Array[String]): Unit = {


    //加上了读取redis的偏移量

    val sc:SparkConf=new SparkConf().setAppName("dau_app").setMaster("local[3]")
    val ssc = new StreamingContext(sc,Seconds(5))
    val topic="GMALL_STARTUP_0103"
    val groupid="DAU_GROUP"
    val kafkaOffsetMap:Map[TopicPartition,Long]=OffsetManager.getOffset(topic,groupid)
    var recordInputStream:InputDStream[ConsumerRecord[String,String]]=null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      recordInputStream=MyKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupid)
    }else{
      recordInputStream=MyKafkaUtil.getKafkaStream(topic,ssc)
    }



    var startupOffsetRanges:Array[OffsetRange]=Array.empty[OffsetRange] //这里仅在启动时执行
    val InputGetOffsetDstream:DStream[ConsumerRecord[String,String]]=recordInputStream.transform{rdd=>
      println("开始读取分区")
      startupOffsetRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges  //这个操作是在driver中执行的  //这里是周期性的执行
      for(offset <- startupOffsetRanges){
        val partition:Int=offset.partition
        val untilOffset:Long=offset.untilOffset
        println("读取分区分区："+partition+":"+offset.fromOffset+"==>"+offset.untilOffset)
      }
      rdd
    }




     /*val sc:SparkConf=new SparkConf().setAppName("dau_app").setMaster("local[3]")
     val ssc =new StreamingContext(sc,Seconds(5))*/
     /*val topic="GMALL_STARTUP_0103"
     val recordInputStream:InputDStream[ConsumerRecord[String,String]]=MyKafkaUtil.getKafkaStream(topic,ssc)*/

     val  jsonObjDstream:DStream[JSONObject]= InputGetOffsetDstream.map{record=>
      val jsonString:String=record.value()
       /*println(jsonString)*/
      val jsonOjb:JSONObject=JSON.parseObject(jsonString)
      val ts:Long=jsonOjb.getLong("ts")
      val datehourString:String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateHour:Array[String] = datehourString.split(" ")
      jsonOjb.put("dt",dateHour(0))
      jsonOjb.put("hr",dateHour(1))
      println(jsonOjb.get("dt"))
      jsonOjb

    }








    //未进行redis连接优化
    /*jsonObjDstream.filter(jsonObj=>{
      val dt:String=jsonObj.getString("dt")
      val mid=jsonObj.getJSONObject("common").getString("mid")
      val jedis:Jedis=RedisUtil.getJedisClient
      val dauKey="dau:"+dt
      val isNew:Long=jedis.sadd(dauKey,mid)
      jedis.close();
      if(isNew==1L){
        true
      }else{
        false
      }
      println(dt)
      true
    })*/





    //进行了redis连接池优化，并在一个partition中只去一次连接对象
    val filterDStream:DStream[JSONObject]=jsonObjDstream.mapPartitions{jsonObjItr=>
      val jedis:Jedis=RedisUtil.getJedisClient
      val filterList=new ListBuffer[JSONObject]()
      val jsonList:List[JSONObject]=jsonObjItr.toList
      println("去重之前"+jsonList.size)
      for(jsonObj <- jsonList){
        val dt:String=jsonObj.getString("dt")
        val mid=jsonObj.getJSONObject("common").getString("mid")
        val dauKey="dau:"+dt
        val isNew:Long=jedis.sadd(dauKey,mid)
        if(isNew==1L){
          filterList+=jsonObj
        }else{
          false
        }
      }
      println("去重之后"+filterList.size)
      jedis.close()   //一个还没实现的想法。。就是如果json是null就别去redis链接了
      println(jsonList.size)
      filterList.toIterator
    }



    /*filterDStream.foreachRDD(x=>
      x.foreach(y=>
        y.getString("dt")
      )
    )*/

    //用bulk批量插入es
    filterDStream.foreachRDD{rdd=>
      println("进入批量插入")
      rdd.foreachPartition{jsonItr=>
        var dauList:List[JSONObject]=jsonItr.toList


        //把源数据转化成要保存的数据格式
        dauList.map{jsonObj=>
          println("进入es封装")
          val commonJSONObj:JSONObject=jsonObj.getJSONObject("common")
          DauInfo(
            commonJSONObj.getString("mid"),
            commonJSONObj.getString("uid"),
            commonJSONObj.getString("ar"),
            commonJSONObj.getString("ch"),
            commonJSONObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            "00",
            jsonObj.getLong("ts")
          )
        }
      dauList

        val dt:String=new SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date())
        MyEsUtil.bulkDoc(dauList,"gamll0105_dau_info_"+dt)
      }
      ///
      //偏移量的提交
      println("即将进入偏移量的提交")
      OffsetManager.saveOffset(topic,groupid,startupOffsetRanges)
      ///
    }

    filterDStream.print()


    ssc.start()
    ssc.awaitTermination()




  }
}
