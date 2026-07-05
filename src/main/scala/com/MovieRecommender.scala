package com  // 改成 com，和你的其他代码一致

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.recommendation.ALS
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

object MovieRecommender {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MovieRecommender")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    import spark.implicits._

    println("=" * 60)
    println("🎬 阶段五：个性化电影推荐系统")
    println("=" * 60)

    // 1. 读取评分数据（使用你的 HDFS 路径）
    println("\n📊 读取评分数据...")
    val ratingsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs://192.168.102.211:9000/data/ratings/ratings.csv")  // 改成你的 IP

    println(s"✅ 加载评分数据: ${ratingsDF.count()} 条")
    ratingsDF.show(5, false)

    // 2. 读取电影数据
    println("\n📊 读取电影数据...")
    val moviesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs://192.168.102.211:9000/data/movies/movies.csv")  // 改成你的 IP

    println(s"✅ 加载电影数据: ${moviesDF.count()} 条")
    moviesDF.show(5, false)

    // 3. 训练 ALS 模型
    println("\n🔄 训练 ALS 协同过滤模型...")
    val als = new ALS()
      .setRank(10)
      .setMaxIter(10)
      .setRegParam(0.1)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setColdStartStrategy("drop")

    val model = als.fit(ratingsDF)
    println("✅ 模型训练完成！")

    // 4. 为所有用户生成 Top 3 推荐
    println("\n🔄 为所有用户生成 Top 3 推荐...")
    val allUsers = ratingsDF.select("userId").distinct()
    val top3Recommendations = model.recommendForUserSubset(allUsers, 3)

    println(s"✅ 推荐结果生成完成！共 ${top3Recommendations.count()} 条")

    // 5. 展开推荐结果
    val flattenedRecs = top3Recommendations
      .select($"userId", explode($"recommendations").as("rec"))
      .select($"userId", $"rec.movieId", $"rec.rating")

    // 6. 关联电影名称
    val recWithMovieName = flattenedRecs
      .join(moviesDF, Seq("movieId"), "left")
      .select($"userId", $"movieId", $"title", $"rating")

    println("\n📊 推荐结果（含电影名称）:")
    recWithMovieName.show(20, false)

    // 7. 统计信息
    val totalUsers = recWithMovieName.select("userId").distinct().count()
    val totalRecs = recWithMovieName.count()
    println(s"\n📊 共为 $totalUsers 个用户生成 $totalRecs 条推荐")

    // 8. 写入 HBase（用你的 HBase 地址）
    println("\n🔄 写入 HBase...")
    writeToHBase(recWithMovieName)

    println("\n" + "=" * 60)
    println("✅ 阶段五完成！所有推荐结果已写入 HBase！")
    println("=" * 60)

    spark.stop()
  }

  def writeToHBase(df: org.apache.spark.sql.DataFrame): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "192.168.102.211")  // 改成你的 IP
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    var connection: org.apache.hadoop.hbase.client.Connection = null
    var table: org.apache.hadoop.hbase.client.Table = null

    try {
      connection = ConnectionFactory.createConnection(conf)
      table = connection.getTable(TableName.valueOf("movie_reco"))

      // 按用户分组
      val grouped = df.collect().groupBy(row => row.getAs[Int]("userId"))

      var totalPuts = 0

      for ((userId, rows) <- grouped) {
        val rowKey = userId.toString
        val put = new Put(Bytes.toBytes(rowKey))

        // 存储格式: movieId:title:rating|movieId:title:rating|movieId:title:rating
        val recStr = rows.take(3).map { row =>
          val movieId = row.getAs[Int]("movieId")
          val title = row.getAs[String]("title")
          val rating = row.getAs[Float]("rating")
          s"$movieId:$title:$rating"
        }.mkString("|")

        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("recommendations"), Bytes.toBytes(recStr))
        table.put(put)
        totalPuts += 1

        if (totalPuts % 5 == 0) {
          println(s"📝 已写入 $totalPuts 个用户的推荐...")
        }
      }

      println(s"✅ 成功写入 $totalPuts 个用户的推荐到 HBase！")

    } catch {
      case e: Exception =>
        println(s"❌ HBase 写入失败: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      if (table != null) table.close()
      if (connection != null) connection.close()
    }
  }
}