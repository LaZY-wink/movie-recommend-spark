package com

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source

object KafkaClickLogProducer {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "192.168.102.211:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "movie_logs"

    // 从 resources 目录读取 CSV 文件
    val source = Source.fromInputStream(getClass.getResourceAsStream("/kafka_click_logs.csv"))
    val lines = source.getLines().drop(1).toList
    source.close()

    println(s"✅ 共加载 ${lines.length} 条点击记录")
    println("⏳ 开始循环发送数据到 Kafka... (按 Ctrl+C 停止)\n")

    var count = 0
    try {
      while (true) {
        for (line <- lines) {
          val Array(movieId, timestamp) = line.split(",")
          val json = s"""{"movieId":$movieId,"timestamp":"$timestamp"}"""
          val record = new ProducerRecord[String, String](topic, json)
          producer.send(record)
          count += 1
          println(s"[$count] 发送: $json")
          Thread.sleep(200) // 200ms 间隔
        }
        println(s"\n--- 第 ${count / lines.length} 轮发送完毕，继续循环 ---\n")
        Thread.sleep(1000)
      }
    } catch {
      case _: InterruptedException =>
        println("\n⏹ 生产者已停止")
    } finally {
      producer.close()
    }
  }
}