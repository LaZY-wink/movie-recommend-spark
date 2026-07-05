# 🎬 基于Spark生态的智能影视推荐与实时分析系统

> 融合**实时流计算**与**离线机器学习**的全链路大数据平台 

---

## 📌 项目亮点

- **Lambda架构实践**：实时层（Structured Streaming）+ 批处理层（Spark SQL/MLlib）分离，互不干扰
- **端到端数据闭环**：模拟数据生成 → Kafka传输 → 流计算聚合 → MySQL存储 → SpringBoot服务 → ECharts展示
- **完整推荐系统**：从ALS模型训练（Spark MLlib）到HBase在线存储，再到RESTful API实时查询

---

## 🛠 技术栈

| 层级 | 技术 | 版本 |
|------|------|------|
| 分布式存储 | Hadoop HDFS + HBase + Hive | 3.x / 2.4.9 / 3.x |
| 消息队列 | Apache Kafka | 2.0.0 |
| 计算引擎 | Apache Spark (SQL + Structured Streaming + MLlib) | 2.4.5 |
| 协调服务 | ZooKeeper | 3.4.x |
| 后端服务 | SpringBoot + MySQL | 2.2.6 / 8.0 |
| 前端可视化 | ECharts + HTML5 + Ajax | - |
| 开发语言 | Scala 2.11 + Java 8 | - |

---

## 🏗 系统架构

```text
┌─────────────────────────────────────────────────────────────────┐
│                         数据源层                               │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐  │
│  │ movies.csv │  │ ratings.csv│  │ kafka_click_logs.csv   │  │
│  │ (电影信息) │  │ (用户评分) │  │ (模拟实时点击流)        │  │
│  └─────┬──────┘  └─────┬──────┘  └───────────┬────────────┘  │
│        │               │                      │               │
│        ▼               ▼                      ▼               │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐  │
│  │    HDFS    │  │    HDFS    │  │    Kafka               │  │
│  │ (离线存储) │  │ (离线存储) │  │ (movie_logs Topic)     │  │
│  └─────┬──────┘  └─────┬──────┘  └───────────┬────────────┘  │
│        │               │                      │               │
│        └───────────────┼──────────────────────┘               │
│                        ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  Spark 计算引擎层                        │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │  │
│  │  │  Spark SQL   │  │ Structured   │  │ Spark MLlib  │  │  │
│  │  │ (离线探索)   │  │ Streaming    │  │ (ALS推荐)    │  │  │
│  │  │ 8维度分析    │  │ (滑动窗口)   │  │ Top-3生成    │  │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                        │                      │               │
│                        ▼                      ▼               │
│  ┌────────────────────┐  ┌──────────────────────────────┐    │
│  │       HBase        │  │          MySQL               │    │
│  │   (推荐结果存储)    │  │      (热门前10排行)           │    │
│  └─────────┬──────────┘  └──────────────┬───────────────┘    │
│            │                             │                    │
│            └──────────────┬──────────────┘                    │
│                           ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              SpringBoot 服务层 (RESTful API)            │  │
│  │  ┌────────────────────┐  ┌───────────────────────────┐ │  │
│  │  │  /api/hot/ranking  │  │ /api/recommend/{userId}   │ │  │
│  │  │  热门排行查询       │  │ 查询用户Top-3推荐          │ │  │
│  │  └────────────────────┘  └───────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              展示层 (ECharts + HTML5)                    │  │
│  │  ┌────────────────────┐  ┌───────────────────────────┐ │  │
│  │  │   实时监控大屏      │  │   个性化推荐页面           │ │  │
│  │  │  (柱状图/5s刷新)   │  │ (输入UserId→展示Top-3)    │ │  │
│  │  └────────────────────┘  └───────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 数据流转链路

| 链路 | 流程 |
|------|------|
| 实时流 | 模拟日志 → Kafka → Streaming（窗口聚合）→ MySQL → SpringBoot → ECharts大屏 |
| 离线批 | CSV → HDFS → Spark SQL（分析）/ ALS（推荐）→ HBase → SpringBoot → 推荐页面 |
## 🚀 核心功能

### 1. 实时热点监控（延迟 ≤ 5s）
- Spark Structured Streaming 消费 Kafka `movie_logs` Topic（3分区2副本）
- 滑动窗口聚合：**窗口10秒，滑动5秒**
- 结果写入 MySQL → SpringBoot → ECharts 柱状图自动刷新

### 2. 个性化推荐（ALS协同过滤）
- Spark MLlib ALS算法：rank=10, maxIter=10, regParam=0.1
- 为全部用户生成 **Top-3** 推荐
- 批量写入 HBase（RowKey=userId, 列族=info）
- SpringBoot RESTful API 实时查询

### 3. 离线数据多维探索（Spark SQL）
- 8个分析维度：电影分类统计 / 单影片口碑 / 用户行为画像 / 评分矩阵稀疏度（87.5%）等
- 为推荐算法选型提供数据支撑

---

## 📁 项目结构

```text
movie-recommend-spark/
│
├── src/main/
│   ├── java/
│   │   └── Controller/
│   │       ├── MovieApplication.java      # SpringBoot启动类
│   │       └── HotMovieController.java    # RESTful API
│   │
│   ├── resources/
│   │   ├── static/
│   │   │   ├── index.html                # ECharts监控大屏
│   │   │   └── recommend.html            # 推荐结果页面
│   │   │
│   │   ├── application.yml               # 配置文件
│   │   ├── movies.csv                    # 电影信息(8部)
│   │   ├── ratings.csv                   # 用户评分(14条)
│   │   └── kafka_click_logs.csv          # 模拟点击日志
│   │
│   └── scala/com/
│       ├── RealTimeHotMovie.scala        # Structured Streaming流计算
│       ├── MovieRecommender.scala        # ALS模型训练+推荐生成
│       └── KafkaClickLogProducer.scala   # Kafka模拟数据生产
│
├── pom.xml                               # Maven依赖
└── README.md                             # 项目说明

---

## 🔧 环境要求

| 组件 | 版本 |
|------|------|
| Hadoop | 3.x |
| ZooKeeper | 3.4.x |
| Kafka | 2.0.0 |
| HBase | 2.4.9 |
| Hive | 3.x |
| Spark | 2.4.5 (Scala 2.11) |
| MySQL | 8.0 |
| JDK | 8+ |
| Maven | 3.x |

---

## ⚡ 快速启动

```bash
# 1. 克隆项目
git clone https://github.com/LaZY-wink/movie-recommend-spark.git
cd movie-recommend-spark

# 2. 打包
mvn clean package

# 3. 启动各大数据组件
# (Hadoop / ZooKeeper / Kafka / HBase / Hive 需提前启动)

# 4. 创建Kafka Topic
kafka-topics.sh --create --topic movie_logs --partitions 3 --replication-factor 2

# 5. 创建HBase表
hbase shell <<< "create 'movie_reco', 'info'"

# 6. 启动模拟数据生产
spark-submit --class com.KafkaClickLogProducer target/movie-recommendation3-1.0.0-SNAPSHOT.jar

# 7. 提交流计算任务
spark-submit --class com.RealTimeHotMovie target/movie-recommendation3-1.0.0-SNAPSHOT.jar

# 8. 训练推荐模型
spark-submit --class com.MovieRecommender target/movie-recommendation3-1.0.0-SNAPSHOT.jar

# 9. 启动SpringBoot服务
mvn spring-boot:run

# 10. 访问
# 监控大屏: http://localhost:8080/index.html
# 推荐页面: http://localhost:8080/recommend.html
