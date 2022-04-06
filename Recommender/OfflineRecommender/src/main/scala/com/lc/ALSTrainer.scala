package com.lc

import breeze.numerics.sqrt
import com.lc.OfflineRecommender.Mysql_RatingTable_Name
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.Properties

//用于选取ALS模型的最优参数
object ALSTrainer {
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

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster(config("spark.cores"))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //加载评分数据
    val ratingRDD: RDD[Rating] = spark.read.jdbc(config("mysql.url"), Mysql_RatingTable_Name, prop).rdd.map(rating => (rating.getAs("uid").toString.toInt, rating.getAs("mid").toString.toInt, rating.getAs("score").toString.toDouble)).map(x => Rating(x._1, x._2, x._3)).cache()

    //随机划分数据集生成训练集和测试集
    val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8, 0.2))

    //训练集
    val trainRDD: RDD[Rating] = splits(0)

    //测试集
    val testRDD: RDD[Rating] = splits(1)

    //模型参数选择，输出最优参数
    adjustALSParam(trainRDD, testRDD)

    //    trainRDD.collect().foreach(println)

    //    testRDD.collect().foreach(println)

    spark.close()
  }

  /**
   * 定义相关选择最优参数方法
   *
   * @param trainData 训练集
   * @param testRDD   测试集
   */
  def adjustALSParam(trainRDD: RDD[Rating], testRDD: RDD[Rating]): Unit = {
    //yield来保存每一次循环的中间结果
    val result: Array[(Int, Double, Double)] = for (rank <- Array(20, 50, 100); lambda <- Array(0.001, 0.01, 0.1))
      yield {
        val model: MatrixFactorizationModel = ALS.train(trainRDD, rank, 50, lambda)
        //计算均方根误差（RMSE）
        val rmse: Double = getRMSE(model, testRDD)
        (rank, lambda, rmse)
      }
    //控制台打印输出最优参数
    println(result.minBy(_._3))
  }


  def getRMSE(model: MatrixFactorizationModel, testRDD: RDD[Rating]) = {
    //计算预测评分
    val userProducts: RDD[(Int, Int)] = testRDD.map(item => (item.user, item.product))

    val predictRating: RDD[Rating] = model.predict(userProducts)

    //以uid，mid作为外键来进行内连接 inner join实际观测值和预测值
    //计算实际的观测值
    val observed: RDD[((Int, Int), Double)] = testRDD.map(item => ((item.user, item.product), item.rating))

    //计算预测值
    val predict: RDD[((Int, Int), Double)] = predictRating.map(item => ((item.user, item.product), item.rating))

    //内连接得到(uid , mid) , (实际值,预测值)
    sqrt(
      observed.join(predict).map {
        case ((uid, mid), (actual, pre)) =>
          val err = actual - pre
          err * err
      }.mean()
    )
  }
}