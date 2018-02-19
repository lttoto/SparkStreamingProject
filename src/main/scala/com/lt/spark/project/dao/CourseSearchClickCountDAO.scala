package com.lt.spark.project.dao

import com.lt.spark.project.domain.{CourseSearchClickCount, CourseClickCount}
import com.lt.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * Created by taoshiliu on 2018/2/19.
  */
object CourseSearchClickCountDAO {

  val tableName = "course_search_clickcount_lt";
  val cf = "info"
  val qualifer = "click_count"

  def save(list:ListBuffer[CourseSearchClickCount]):Unit = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    for(ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_course),Bytes.toBytes(cf),Bytes.toBytes(qualifer),ele.click_count)
    }
  }

  def count(day_search_course:String):Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_search_course))
    val value = table.get(get).getValue(cf.getBytes,qualifer.getBytes)

    if(value == null) {
      0L
    }else{
      Bytes.toLong(value)
    }
  }
}
