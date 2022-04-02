package com.lc

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.util.Properties


import java.text.SimpleDateFormat
import java.util.Date

object StatisticsRecommender {
  //定义表名
  val Mysql_MovieTable_Name = "movie"
  val Mysql_RatingTable_Name = "rating"

  //统计表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies" //历史热门统计
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies" //最近热门统计
  val AVERAGE_MOVIES = "AverageMovies" //平均评分统计
  val GENRES_TOP_MOVIES = "GenresTopMovies" //类别top10统计

  //定义相关的配置
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
    val conf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取数据转换成DataFrame
    val ratingDF: DataFrame = spark.sqlContext.read.jdbc(config("mysql.url"), Mysql_RatingTable_Name, prop);
    val movieDF: DataFrame = spark.sqlContext.read.jdbc(config("mysql.url"), Mysql_MovieTable_Name, prop);

    import spark.implicits._


    //创建名为ratings的临时视图
    ratingDF.createOrReplaceTempView("ratings")

    //todo ：不同的统计推荐结果
    //1.历史热门电影统计:根据所有历史评分数据，计算历史评分次数最多的电影。
    //mid , count
    val rateMoreMoviesDF: DataFrame = spark.sql("select mid , count(mid) as count from ratings group by mid")
    //    storeDFtoMySql(rateMoreMoviesDF, RATE_MORE_MOVIES)


    //2.最近热门电影统计:根据评分，按"yyyyMM"月为单位计算最近时间的月份里面评分数最多的电影集合。
    //创建日期格式化工具
    val simpleDataFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMM")

    //注册一个udf函数将时间戳转换成想要的yyyyMM格式
    spark.udf.register("changeDate", (timestamp: Int) => simpleDataFormat.format(new Date(timestamp * 1000L)).toInt)

    //对原始数据做预处理，去掉uid
    val ratingOfYearMonth: DataFrame = spark.sql("select mid , score , changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    //从临时表中查找电影在各个月份的评分统计，mid , count , yearmonth
    val rateMoreRecentlyMovies: DataFrame = spark.sql("select mid , count(mid) as count , yearmonth from  ratingOfMonth group by mid , yearmonth order by yearmonth desc , count desc")

    //存储到MySQL中
    //    storeDFtoMySql(rateMoreRecentlyMovies, RATE_MORE_RECENTLY_MOVIES)


    //3.电影平均得分统计:根据历史数据中所有用户对电影的评分，周期性的计算每个电影的平均得分。
    //mid , avg
    val averageMoviesDF: DataFrame = spark.sql("select mid , avg(score) as score from ratings group by mid")

    //存储到MySQL中
    //    storeDFtoMySql(averageMoviesDF, AVERAGE_MOVIES)


    //4.每个类别优质电影统计:根据提供的所有电影类别，分别计算每种类型的电影集合中评分最高的 10 个电影。
    //定义出所有的类别
    val genres: List[String] = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery", "Romance", "Science", "Tv", "Thriller", "War", "Western")

    //把平均评分加入到电影表中，加一列
    val movieWithScore: DataFrame = movieDF.join(averageMoviesDF, "mid")

    //做笛卡尔积，将genres转换成rdd
    val genresRDD: RDD[String] = spark.sparkContext.makeRDD(genres)


    //用来拼接字符串
    val builder: StringBuilder = new StringBuilder

    //计算类别的top10,首先对类别和电影做笛卡尔积
    val genresTopMoviesDF: DataFrame = genresRDD.cartesian(movieWithScore.rdd)
      .filter {
        //找出movie的字段genres值包含当前类别的那些，表示符合要求
        case (genre, row) => row.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }
      .map {
        case (genre, row) => (genre, (row.getAs[Int]("mid"), row.getAs[Double]("score")))
      }
      .groupByKey()
      .map {
        //        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => builder.append("{\"mid\":" + item._1 + "," + "\"score\":" + item._2 + "}").toString())
        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => "{\"mid\":" + item._1 + "," + "\"score\":" + item._2 + "}").toString())
      }.toDF()

    //存储到MySQL
    storeDFtoMySql(genresTopMoviesDF, GENRES_TOP_MOVIES)

    //关闭资源
    spark.stop();
  }

  //存储DF到MySQL中
  def storeDFtoMySql(df: DataFrame, tableName: String): Unit = {
    df.write.jdbc(config("mysql.url"), tableName, prop)
  }

  //定义样例类
  case class Movie(mid: Int, name: String, descri: String, timelog: String, issue: String, shoot: String, language: String,
                   genres: String, actors: String, directors: String)

  case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

  //定义基准推荐对象
  case class Recommendation(mid: Int, score: Double)

  //定义电影类别top10推荐样例类
  case class GenresRecommendation(genres: String, recs: String)

}
