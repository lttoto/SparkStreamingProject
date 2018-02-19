package com.lt.spark.project.spark

import com.lt.spark.project.dao.{CourseSearchClickCountDAO, CourseClickCountDAO}
import com.lt.spark.project.domain.{CourseSearchClickCount, CourseClickCount, ClickLog}
import com.lt.spark.project.util.DataUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by taoshiliu on 2018/2/19.
  */
object LtStatStreamingApp {

  def main(args: Array[String]) {

    if(args.length != 4) {
      System.err.println("Usage")
    }

    //数据采集
    val Array(zkQuorum,group,topics,numThreads) = args

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("LtStatStreamingApp")
    val ssc = new StreamingContext(sparkConf,Seconds(60))

    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    //kafkaStream.map(_._2).count().print()

    //数据清洗
    //29.124.132.143	2018-02-19 05:30:01	"GET /course/list HTTP/1.1"	500	https://www.sogou.com/web?query=Storm实战
    val logs = kafkaStream.map(_._2)
    val cleanData = logs.map(line => {
      val infos = line.split("\t")
      val url =infos(2).split(" ")(1)
      var courseId = 0

      if(url.startsWith("/class")) {
        val courseIdHtml = url.split("/")(2)
        courseId = courseIdHtml.substring(0,courseIdHtml.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0),DataUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))
    }).filter(clicklog => clicklog.courseId != 0)
    //清洗结果ClickLog(187.98.30.10,20180219052101,128,404,-)
    //       ClickLog(156.132.167.187,20180219052101,128,404,http://cn.bing.com/search?q=Spark Streaming实战)

    //cleanData.print()

    //数据处理
    cleanData.map(x => {
      (x.time.substring(0,8) + "_" + x.courseId,1)
    }).reduceByKey(_+_).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]
        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1,pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })

    cleanData.map(x => {
      val referer = x.referer.replaceAll("//","/")
      val splits = referer.split("/")
      var host = ""
      if(splits.length > 2) {
        host = splits(1)
      }

      (host,x.courseId,x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0,8) + "_" + x._1 + "_" + x._2,1)
    }).reduceByKey(_+_).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseSearchClickCount]
        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1,pair._2))
        })

        CourseSearchClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
