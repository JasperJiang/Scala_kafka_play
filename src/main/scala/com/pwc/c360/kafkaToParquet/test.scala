package com.pwc.c360.kafkaToParquet

import com.fasterxml.jackson.databind.ObjectMapper
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

object test {
  def main(args: Array[String]) {
    val jsonUtil = new com.pwc.c360.kafkaToParquet.JsonUtil

    var a = "{\"a\":{\"value\":123,\"type\":\"int\",\"comment\":\"asdasd\"},\"b\":{\"value\":234,\"type\":\"int\",\"comment\":\"asdasd\"},\"c\":213}"
    var b = jsonUtil.json2Map(a)

    val c = b.map(x => {
      val key = x._1
      val value = x._2.get("value")
      key -> value
    })

    val column = b.map(x => {
      val key = x._1
      val col = x._1 + " " + x._2.get("type") + " " + x._2.get("comment")
      key -> col
    })

    val res = Json(DefaultFormats).write(c)

    println(res)

    //    val jsonString = jsonUtil.map2Json(c)

    //    println(jsonUtil.map2Json(c))


    column.foreach(print)
  }
}
