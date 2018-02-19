package com.lt.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by taoshiliu on 2018/2/19.
  * 黑名单实例
  * 20180808,zs
  * 20180808,ls
  * 20180808,ww
  */
object TransformApp {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //构建黑名单
    val blacks = List("zs","ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x=>(x,true))
    /*
    * zs:true
    * ls:true
    * */

    val lines = ssc.socketTextStream("localhost",6789)
    val result = lines.map(x=>(x.split(",")(1),x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD).filter(x=>x._2._2.getOrElse(false)!=true).map(x=>x._2._1)
    })
    /*
    * (zs:20180808,zs)(ls:20180808,ls)(ww:20180808,ww)
    * leftjoin
    * (zs:[<20180808,zs>,<true>])....
    * */
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
