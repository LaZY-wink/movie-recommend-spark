package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import java.util.Properties

object RealTimeHotMovie {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealTimeHotMovie")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    import spark.implicits._

    println("========================================")
    println("=== 1. 开始连接Kafka ===")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.102.211:9092")
      .option("subscribe", "movie_logs")
      .option("startingOffsets", "earliest")  // ← 改成 earliest
      .option("failOnDataLoss", "false")
      .load()

    println("=== Kafka 连接配置完成 ===")

    // 使用数据中的 timestamp 字段
    val clickDF = kafkaDF.selectExpr("CAST(value AS STRING) as json")
      .select(
        get_json_object($"json", "$.movieId").cast("Int").as("movieId"),
        to_timestamp(get_json_object($"json", "$.timestamp")).as("clickTime")
      )
      .filter($"movieId".isNotNull)
      .filter($"clickTime".isNotNull)

    println("=== JSON解析配置完成 ===")

    // 窗口聚合（10秒窗口，5秒滑动）
    // ✅ 删除 withWatermark，因为数据是 2024 年的
    val windowedCounts = clickDF
      .groupBy(
        window($"clickTime", "10 seconds", "5 seconds"),
        $"movieId"
      )
      .agg(count("*").as("clickCount"))
      .orderBy($"clickCount".desc)

    println("=== 2. 窗口聚合配置完成 ===")

    // 控制台输出
    val consoleQuery = windowedCounts.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())  // Complete 模式
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    println("=== 3. 控制台输出已启动 ===")

    // MySQL 配置
    val props = new Properties()
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    props.setProperty("user", "root")
    props.setProperty("password", "Root123456!")

    val jdbcUrl = "jdbc:mysql://192.168.102.211:3306/movie_db?useSSL=false&serverTimezone=Asia/Shanghai"

    println(s"=== MySQL 连接配置: $jdbcUrl ===")

    // MySQL 写入
    val mysqlQuery = windowedCounts.writeStream
      .foreachBatch { (batchDF, batchId) =>
        val count = batchDF.count()
        println(s"=== 🔍 Batch $batchId 数据量: $count ===")

        if (count > 0) {
          batchDF.show(false)

          try {
            val writeDF = batchDF.select(
              col("window.start").cast("timestamp").as("window_start"),
              col("window.end").cast("timestamp").as("window_end"),
              col("movieId").as("movie_id"),
              col("clickCount").as("click_count")
            )

            writeDF.write
              .mode("append")
              .jdbc(jdbcUrl, "hot_movie_ranking", props)

            println(s"✅ Batch $batchId 写入MySQL成功，追加 ${count} 条")

          } catch {
            case e: Exception =>
              println(s"❌ Batch $batchId 写入MySQL失败: ${e.getMessage}")
              e.printStackTrace()
          }
        } else {
          println(s"⚠️ Batch $batchId 为空，跳过写入")
        }
      }
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    println("========================================")
    println("=== 所有组件已启动，等待数据... ===")
    println("========================================")

    spark.streams.awaitAnyTermination()
  }
}