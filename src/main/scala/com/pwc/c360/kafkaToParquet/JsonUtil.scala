package com.pwc.c360.kafkaToParquet

import java.util

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

import scala.collection.mutable

class JsonUtil {
  def map2Json(map : util.Map[String,Object]) : String = {

    val jsonString = JSONObject.toJSONString(map)

    jsonString
  }

  def json2Map(json : String) : mutable.HashMap[String,util.HashMap[String, Object]] = {

    val map: mutable.HashMap[String, util.HashMap[String, Object]] = mutable.HashMap()

    val jsonParser = new JSONParser()

    //将string转化为jsonObject
    val jsonObj: JSONObject = jsonParser.parse(json).asInstanceOf[JSONObject]

    //获取所有键
    val jsonKey = jsonObj.keySet()

    val iter = jsonKey.iterator()

    while (iter.hasNext) {
      val field = iter.next()
      val value = jsonObj.get(field).toString

      if (value.startsWith("{") && value.endsWith("}")) {
        //        println(jsonObj.get(field))
        val value = jsonObj.get(field).asInstanceOf[util.HashMap[String, Object]]
        map.put(field, value)
      }else{
        val defaultValue = "{" + "\"value\":" + jsonObj.get(field).toString  + ",\"type\":\"String\",\"comment\":\"\"}"

        val value = jsonParser.parse(defaultValue).asInstanceOf[JSONObject].asInstanceOf[util.HashMap[String, Object]]
        map.put(field, value)
      }
    }
    map
  }



}
