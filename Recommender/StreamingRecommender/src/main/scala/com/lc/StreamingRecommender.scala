package com.lc

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


//定义连接助手对象
object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("localhost")
}

object StreamingRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MySql_STREAM_RECS_COLLECTION = "StreamRecs"
  val MySql_RATING_COLLECTION = "rating"
  val MySql_MOVIE_RECS_COLLECTION = "MovieRecs"

  val config = Map(
    "spark.cores" -> "local[*]",
    "mysql.url" -> "jdbc:mysql://localhost:3306/recommendsystem",
    "mysql.driver" -> "com.mysql.jdbc.Driver",
    "mysql.user" -> "root",
    "mysql.password" -> "000000",
    "kafka.topic" -> "movierecommender"
  )

  val prop = new Properties()
  prop.put("driver", config("mysql.driver"))
  prop.put("user", config("mysql.user"))
  prop.put("password", config("mysql.password"))

  val conf: SparkConf = new SparkConf().setMaster(config("spark.cores"))
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  //获取streaming context
  val sc: SparkContext = spark.sparkContext
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

  def main(args: Array[String]): Unit = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
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

    //    //声明广播变量
    val simMovieMatrixBroadCast: Broadcast[collection.Map[Int, mutable.LinkedHashMap[Int, Double]]] = sc.broadcast(simMovieMatrix)

    //定义kafka的连接参数
    //    val kafkaParam: Map[String, io.Serializable] = Map(
    //      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    //      "key.deserializer" -> classOf[StringDeserializer],
    //      "value.deserializer" -> classOf[StringDeserializer],
    //      "group.id" -> "movierecommender",
    //      "auto.offset.reset" -> "latest" //偏移量的初始设置
    //    )
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


    //    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
    //        LocationStrategies.PreferConsistent,
    //        ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara))

    //    将数据转换成评分流 uid|mid|score|timestamp
    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map {
      msg => {
        val attr: Array[String] = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      }
    }

    //核心算法部分
    ratingStream.foreachRDD(
      rdds => {
        rdds.foreach {
          case (uid, mid, score, timestamp) => {
            import com.lc.ConnHelper.jedis
            println("rating data coming *************")
            //1.从redis中获取当前用户最近的k次评分，保存成Array[(mid , score)]
            val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRatings(MAX_USER_RATINGS_NUM, uid, jedis)

            //2.从相似度矩阵中取出当前电影最相似的N个电影，作为备选列表Array[mid]
            //得取出用户已经看过的电影，将其剔除
            val candidateMovies: Array[Int] = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

            //3.对每个备选电影计算推荐优先级，得到当前用户的实时推荐列表，Array[mid , score]
            val streamRecs: Array[(Int, Double)] = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)

            //4.把推荐数据保存到MySQL中
            storeDataToMySql(uid, streamRecs)
          }
        }
      }
    )

    //开始接受和处理数据
    ssc.start()

    println("***************streaming started")
    ssc.awaitTermination()
  }

  //为了redis操作返回是java类，为了使用map操作需要我们引入转换类
  //将java类型转换成scala类型

  import scala.collection.JavaConversions._

  def getUserRecentlyRatings(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    //从redis中读取数据，用户评分保存在uid:UID为key的队列里。value是MID:SCORE
    jedis.lrange("uid:" + uid, 0, num - 1).map {
      item => { //具体每一个评分又是以冒号分割的两个值
        val attr: Array[String] = item.split(":")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
    }.toArray
  }

  /**
   * 获取和当前电影最相似的num个电影，作为备选电影
   *
   * @param num   相似电影的数量
   * @param mid   当前电影的id
   * @param uid   当前评分用户的uid
   * @param value 相似度矩阵
   * @return 过滤之后的备选电影列表
   */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, mutable.LinkedHashMap[Int, Double]]): Array[Int] = {
    //1.从相似度矩阵中拿到所有相似的电影
    val allSimMovies: Array[(Int, Double)] = simMovies(mid).toArray

    //2.从MySQL中查询当前uid已经看过的电影
    val movies: DataFrame = spark.read.jdbc(config("mysql.url"), MySql_RATING_COLLECTION, prop)

    //得到用户已经看过的电影mid的列表
    //todo 重构此处
    val ratingExist: Array[Int] = movies.select("mid").where("uid = " + uid).toDF().rdd.map {
      line => line.getAs("mid").toString.toInt
    }.collect()

    //3.把看过的过滤，得到输出列表
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)
  }

  /**
   * 计算电影推荐优先值
   *
   * @param candidateMovies     当前电影最相似的N个电影，备选电影
   * @param userRecentlyRatings 用户最近评分
   * @param simMovies           电影相似度矩阵
   * @return 计算电影推荐优先值
   */
  def computeMovieScores(candidateMovies: Array[Int], userRecentlyRatings: Array[(Int, Double)], simMovies: collection.Map[Int, mutable.LinkedHashMap[Int, Double]]): Array[(Int, Double)] = {
    //定义一个ArrayBuffer用于保存每一个备选电影的基础得分
    val scores: ArrayBuffer[(Int, Double)] = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    //定义一个HashMap，保存每一个备选电影的增强减弱因子
    val increaMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int, Int]()
    val decreaMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int, Int]()

    for (candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {
      //拿到备选电影和最近评分电影的相似度
      val simScore: Double = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)
      //当相似度大于0.7
      if (simScore > 0.7) {
        //计算备选电影的基础推荐得分
        //按照公式来计算
        scores += ((candidateMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increaMap(candidateMovie) = increaMap.getOrDefault(candidateMovie, 0) + 1
        } else {
          decreaMap(candidateMovie) = decreaMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }
    //根据备选电影的mid做groupby,根据公式去求最后的推荐评分
    scores.groupBy(_._1).map {
      //groupby之后得到的数据是Map(mid -> ArrayBuffer[(mid , score)])
      case (mid, scoreList) => {
        (mid, scoreList.map(_._2).sum / scoreList.length + Math.log10(increaMap.getOrDefault(mid, 1)) - Math.log10(decreaMap.getOrDefault(mid, 1)))
      }
    }.toArray.sortWith(_._2 > _._2)
  }

  //获取两个电影之间的相似度
  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: collection.Map[Int, mutable.LinkedHashMap[Int, Double]]): Double = {
    simMovies.get(mid1) match {
      //如果mid1能取到值的话，再判断mid2，如果mid2能取到值，则直接返回结果
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
      }
    }
  }

  /**
   * 将数据保存到MySQL中
   *
   * @param uid        用户id
   * @param streamRecs 推荐列表
   */
  def storeDataToMySql(uid: Int, streamRecs: Array[(Int, Double)]): Unit = {
    import spark.implicits._
    //根据传入的数据构建dataframe
    Seq((uid, streamRecs.toString)).toDF("uid", "recs").write.jdbc(config("mysql.url"), MySql_STREAM_RECS_COLLECTION, prop)
  }

}

//定义基于隐语义模型电影特征向量的电影相似度列表(相似度矩阵)
case class MovieRecs(mid: Int, recs: String)

//定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)

//定义基于预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: String)