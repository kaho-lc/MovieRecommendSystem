package com.lc

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix
import org.apache.spark.sql.SaveMode
import java.util.Properties


//基于评分数据的隐语义模型,只需要rating数据
object OfflineRecommender {
  val config = Map(
    "spark.cores" -> "local[*]",
    "mysql.url" -> "jdbc:mysql://localhost:3306/recommendsystem",
    "mysql.driver" -> "com.mysql.jdbc.Driver",
    "mysql.user" -> "root",
    "mysql.password" -> "000000",
  )

  val prop = new Properties()
  prop.put("driver", config("mysql.driver"))
  prop.put("user", config("mysql.user"))
  prop.put("password", config("mysql.password"))

  // 定义常量
  val Mysql_RatingTable_Name = "rating"
  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"
  //定义用户最大推荐列表个数
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //从数据库中加载数据,这里取出三项(mid , uid , score)
    val ratingRDD: RDD[(Int, Int, Double)] = spark.read.jdbc(config("mysql.url"), Mysql_RatingTable_Name, prop).rdd.map(rating => (rating.getAs("uid").toString.toInt, rating.getAs("mid").toString.toInt, rating.getAs("score").toString.toDouble)).cache() //持久化到内存中

    //从rating数据中提取所有的uid和mid并且去重，再进行缓存
    val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct().cache()
    val movieRDD: RDD[Int] = ratingRDD.map(_._2).distinct().cache()

    //训练隐语义模型
    //将数据包装成spark mllib提供的Rating类型
    val trainData: RDD[Rating] = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    //rank:隐特征向量的维度
    //iterations:迭代的次数
    val model: MatrixFactorizationModel = ALS.train(ratings = trainData, 50, 5, 0.01)

    //基于用户和电影的隐特征计算预测评分，得到用户推荐列表
    //计算用户和电影的笛卡尔积，得到一个空的评分矩阵
    val userMovies: RDD[(Int, Int)] = userRDD.cartesian(movieRDD)

    //调用model的predict方法来进行预测评分
    val preRatings: RDD[Rating] = model.predict(userMovies)

    import spark.implicits._

    val userRecs: DataFrame = preRatings
      .filter(_.rating > 0) //过滤出评分大于0的项
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(item => Recommendation(item._1, item._2)).toString())
      }.toDF()

    //将最终的结果存储到MySQL中
    //mode(SaveMode.Overwrite)设置数据保存模式为覆盖
    //    userRecs.write.mode(SaveMode.Overwrite).jdbc(config("mysql.url"), USER_RECS, prop)


    //基于电影隐特征计算相似度矩阵，得到电影的相似度列表
    //拿出电影的特征
    val movieFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
      case (mid, features) => (mid, new DoubleMatrix(features))
    }

    //对所有的电影自己和自己做笛卡尔积
    val movieRecs: DataFrame = movieFeatures.cartesian(movieFeatures).filter {
      //将自己和自己所配对的情况过滤掉
      case (a, b) => a._1 != b._1
    }.map {
      case (a, b) => {
        //计算余弦相似度
        val simScore: Double = this.consinSim(a._2, b._2)
        (a._1, (b._1, simScore))
      }
    }.filter(x => x._2._2 > 0.6) //过滤出相似度大于0.6的
      .groupByKey()
      .map {
        case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2).map(item => Recommendation(item._1, item._2)).toString())
      }.toDF()

    //将电影的相似度结果保存到MySQL
    movieRecs.write.mode(SaveMode.Overwrite).jdbc(config("mysql.url"), MOVIE_RECS, prop)

    spark.stop()
  }

  //计算向量的余弦相似度
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    //分子:点乘
    //norm2 l2范数代表的就是向量模长
    //分母:向量模长的乘积
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }
}

//定义样例类
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

//定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)

//定义基于预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: String)

//定义基于隐语义模型电影特征向量的电影相似度列表(相似度矩阵)
case class MovieRecs(mid: Int, recs: String)