package com.lt.spark.project.dao

import com.lt.spark.project.domain.CourseClickCount
import com.lt.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Created by taoshiliu on 2018/2/19.
  */
object CourseClickCountDAO {

  val tableName = "course_clickcount_lt";
  val cf = "info"
  val qualifer = "click_count"

  def save(list:ListBuffer[CourseClickCount]):Unit = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    for(ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),Bytes.toBytes(cf),Bytes.toBytes(qualifer),ele.click_count)
    }
  }

  def count(day_course:String):Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes,qualifer.getBytes)

    if(value == null) {
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]) {
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20171111_8",8))
    list.append(CourseClickCount("20171111_9",88))
    list.append(CourseClickCount("20171111_1",800))

    save(list)
    println(count("20171111_8") + " : " + count("20171111_9") + " : " + count("20171111_1"))
  }
}
