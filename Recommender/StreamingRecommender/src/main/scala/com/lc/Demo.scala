package com.lc

import com.lc.StreamingRecommender._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object Demo {
  val config = Map(
    "spark.cores" -> "local[*]",
    "mysql.url" -> "jdbc:mysql://localhost:3306/recommendsystem",
    "mysql.driver" -> "com.mysql.jdbc.Driver",
    "mysql.user" -> "root",
    "mysql.password" -> "000000",
    "kafka.topic" -> "movierecommender"
  )

  def main(args: Array[String]): Unit = {
    import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
    import org.apache.spark.streaming.dstream.{DStream, InputDStream}
    import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
    val conf: SparkConf = new SparkConf().setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //加载电影的相似度矩阵数据，把他广播出去，为了性能的考虑
    val value: RDD[(String, String)] = spark.read.jdbc(config("mysql.url"), MySql_MOVIE_RECS_COLLECTION, prop).rdd.map(line => (line.getAs("mid").toString, line.getAs("recs").toString))

    val simMovieMatrix: scala.collection.Map[Int, mutable.LinkedHashMap[Int, Double]] = value.map {
      case (mid, recs) => {
        //去除字符串中不需要的字符
        val str: String = recs.replace("List(", "").replace(" Recommendation", "").replace("Recommendation", "")
        val str1: String = str.substring(0, str.length - 1)
        val hashMap: mutable.LinkedHashMap[Int, Double] = new mutable.LinkedHashMap[Int, Double]()
        str1.split("\\),").foreach(
          line => {
            val strings: Array[String] = line.replace("(", "").replace(")", "").split(",")
            hashMap += ((strings(0).toInt, strings(1).toDouble))
          }
        )
        (mid.toInt, hashMap)
      }
    }.collectAsMap()

    //声明广播变量
    val simMovieMatrixBroadCast: Broadcast[collection.Map[Int, mutable.LinkedHashMap[Int, Double]]] = sc.broadcast(simMovieMatrix)


    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "movierecommender",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "latest" //偏移量的初始设置
    )

    //    通过kafka创建一个DStream
    //    LocationStrategies存储策略
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(config("kafka.topic")), kafkaPara))


    //    将数据转换成评分流 uid|mid|score|timestamp
    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map {
      msg => {
        val attr: Array[String] = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      }
    }

//    //核心算法部分
//    ratingStream.foreachRDD(
//      rdds => {
//        rdds.foreach {
//          case (uid, mid, score, timestamp) => {
//            import com.lc.ConnHelper.jedis
//            println("rating data coming *************")
//
//            //1.从redis中获取当前用户最近的k次评分，保存成Array[(mid , score)]
//            val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRatings(MAX_USER_RATINGS_NUM, uid, jedis)
//
//            for (elem <- userRecentlyRatings) {
//              println(elem)
//            }
//            //2.从相似度矩阵中取出当前电影最相似的N个电影，作为备选列表Array[mid]
//            //得取出用户已经看过的电影，将其剔除
//            val candidateMovies: Array[Int] = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)
//            for (elem <- candidateMovies) {
//              println(elem)
//            }
//            //3.对每个备选电影计算推荐优先级，得到当前用户的实时推荐列表，Array[mid , score]
//            val streamRecs: Array[(Int, Double)] = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)
//
//            for (elem <- streamRecs) {
//              println(elem._1 + "->" + elem._2)
//            }
////            //4.把推荐数据保存到MySQL中
////            storeDataToMySql(uid, streamRecs)
//          }
//        }
//      }
//    )

    //    val result: Array[Int] = getTopSimMovies(MAX_SIM_MOVIES_NUM, 31, 1, simMovieMatrixBroadCast.value)
    //    for (elem <- result) {
    //      println(elem)
    //    }

    //开始接受和处理数据
    ssc.start()

    println("***************streaming started")
    ssc.awaitTermination()
  }

  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, mutable.LinkedHashMap[Int, Double]]): Array[Int] = {
    import java.sql.{Connection, DriverManager, ResultSet}
    //1.从相似度矩阵中拿到所有相似的电影
    val allSimMovies: Array[(Int, Double)] = simMovies(mid.toInt).toArray

    //2.从MySQL中查询当前uid已经看过的电影
    //    val movies: DataFrame = spark.read.jdbc(config("mysql.url"), MySql_RATING_COLLECTION, prop)

    //得到用户已经看过的电影mid的列表
    //    todo 重构此处
    //    val ratingExist: Array[Int] = movies.select("mid").where(s"uid = ${uid}").toDF().rdd.map {
    //      line => line.getAs("mid").toString.toInt
    //    }.collect()
    //
    //    ratingExist.foreach(println)

    //注册Driver
    Class.forName(config("mysql.driver"))
    //得到连接
    val connection: Connection = DriverManager.getConnection(config("mysql.url"), config("mysql.user"), config("mysql.password"))
    val sql = s"select mid from rating where uid = ${uid}"

    val statement = connection.prepareStatement(sql)

    //执行查询语句，并返回结果
    val resultSet: ResultSet = statement.executeQuery()
    val ratingExist: Array[Int] = Array[Int]()
    while (resultSet.next()) {
      val i: Int = resultSet.getInt("mid")
      ratingExist :+ i
    }
    //关闭连接
    connection.close()

    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)

  }
}
