package com.pwc.c360.kafkaToParquet

import java.util

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

object Main {

  def main(args: Array[String]) {

    var brokers:String = ""
    var topicsArgs:String = ""

    args.sliding(2, 2).toList.collect {
      case Array("brokers", brokersStr: String) => brokers = brokersStr
      case Array("topics", topicsStr: String) => topicsArgs = topicsStr
    }

    val conf = new SparkConf().setAppName("Simple Application")
    val streamingContext = new StreamingContext(SparkContext.getOrCreate(conf), Seconds(5))

    val customCallback = new DisplayOffsetCommits

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "process_topic_test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = topicsArgs.split(",")
    args.foreach(println(_))

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val msgs = rdd.map(x => (x.topic(),x.key(),x.value())).collect()

      msgs.groupBy(x => {x._1}).foreach(msg => {
        val topic = msg._1
        val sameTopics = msg._2

        sameTopics.groupBy(x => {x._2}).foreach(sameTopicKey => {
          val key = sameTopicKey._1
          sameTopicKey._2.foreach(row => {
            val rowValue = row._3
            process(rowValue)
          })
        })
      })


      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges,customCallback)
    }
    streamingContext.start()
    streamingContext.awaitTermination()

  }

  def process(value: String): Unit ={
    println(value)
    val jsonUtil = new JsonUtil
      var b = jsonUtil.json2Map(value)

      val c = b.map(x => {
        val key = x._1
        val value = x._2.get("value")
        key -> value
      })

    val res = Json(DefaultFormats).write(c)
    println(c)
  }

  class DisplayOffsetCommits extends OffsetCommitCallback {
    override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
      println(offsets)
      println(exception)
    }
  }
}
