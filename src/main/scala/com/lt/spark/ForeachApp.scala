package com.lt.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by taoshiliu on 2018/2/19.
  * SparkStreaming 将统计结果存入Mysql
  */
object ForeachApp {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //使用statful算子，必须使用checkpoint
    //生产环境中设置到HDFS目录中
    /*ssc.checkpoint(".")*/

    val lines = ssc.socketTextStream("localhost",6789)
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    /*val stat = result.updateStateByKey[Int](updateFunction _)*/
    /*stat.print()*/

    //错误会导致序列化问题
    /*result.foreachRDD(rdd => {
      val connection = createConnection()
      rdd.foreach{ record =>
        val sql ="insert into wordcount(word,wordcount) values ('" + record._1 + "'," + record._2 + ")"
        connection.createStatement().execute(sql)
      }
    })*/

    result.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql ="insert into wordcount(word,wordcount) values ('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/lt_spark","root","apple111")
  }
  /*def updateFunction(currentValues:Seq[Int],preValues:Option[Int]):Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }*/
}