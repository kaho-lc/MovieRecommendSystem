package com.lc

object demo02 {
  def main(args: Array[String]): Unit = {
    import com.lc.StreamingRecommender.config
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.{SparkConf, SparkContext}

    import java.sql.{Connection, DriverManager, ResultSet}
    import java.util.Properties
    val conf: SparkConf = new SparkConf().setMaster("local[2]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val prop = new Properties()
    prop.put("driver", config("mysql.driver"))
    prop.put("user", config("mysql.user"))
    prop.put("password", config("mysql.password"))

    def getUserRecentlyRatings(uid: Int): Array[(Int , Double)] = {
      import java.util
      import scala.collection.JavaConversions.asScalaBuffer
      //注册Driver
      Class.forName(config("mysql.driver"))
      //得到连接
      val connection: Connection = DriverManager.getConnection(config("mysql.url"), config("mysql.user"), config("mysql.password"))
      val sql = s"select mid , score from rating where uid = ${uid} order by timestamp desc"
      val statement = connection.prepareStatement(sql)
      //执行查询语句，并返回结果
      val resultSet: ResultSet = statement.executeQuery()
//      val mids: Array[Int] = Array[Int]()
      val mids = new util.ArrayList[Int]()
      val scores = new util.ArrayList[Double]()
//      val scores: Array[Double] = Array[Double]()
      while (resultSet.next()) {
        val mid: Int = resultSet.getInt("mid")
        val score: Double = resultSet.getDouble("score")
        mids.add(mid)
        scores.add(score)
      }

      connection.close()
      mids.zip(scores).toArray
    }

    spark.stop()
  }
}
