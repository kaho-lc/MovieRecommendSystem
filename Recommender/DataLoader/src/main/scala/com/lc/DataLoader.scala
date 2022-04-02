package com.lc

import org.apache.http.HttpHost
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest


import java.util.Properties

//数据加载模块
object DataLoader {

  //  定义数据路径常量
  val movieDataPath = "D:\\project\\MovieRecommendSystem\\Recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val ratingDataPath = "D:\\project\\MovieRecommendSystem\\Recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val tagsDataPath = "D:\\project\\MovieRecommendSystem\\Recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val Mysql_MovieTable_Name = "movie"
  val Mysql_RatingTable_Name = "rating"
  val Mysql_TagTable_Name = "tag"
  val ES_MOVIE_INDEX = "Movie"

  //定义相关的配置
  val config = Map(
    "spark.cores" -> "local[*]",
    "es.httpHosts" -> "hadoop102:9200",
    "es.transportHosts" -> "hadoop102:9300",
    "es.index" -> "recommender",
    "es.cluster.name" -> "cluster-es",

    "mysql.url" -> "jdbc:mysql://localhost:3306/recommendsystem",
    "mysql.driver" -> "com.mysql.jdbc.Driver",
    "mysql.user" -> "root",
    "mysql.password" -> "000000",
  )

  def main(args: Array[String]): Unit = {
    //创建一个sparkconfig对象
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    //操作ES的相关配置
    //    sparkConf.set("es.nodes", "localhost")
    //    sparkConf.set("es.port", "9200")
    //    sparkConf.set("es.index.auto.create", "true")
    //    sparkConf.set("es.write.operation", "index")

    //创建sparksession
    val spark: SparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()

    //引入词包，用于将RDD转换成DataFrame
    import spark.implicits._


    //加载数据
    val movieRDD = spark.sparkContext.textFile(movieDataPath);

    //转换成DataFrame
    val movieDF: DataFrame = movieRDD.map(
      line => {
        val attr: Array[String] = line.split("\\^") //需要对^进行转义使用\,但是还需要对\进行转义
        //        封装成样例类
        Movie(attr(0).toInt, attr(1).trim(), attr(2).trim(), attr(3).trim(), attr(4).trim(), attr(5).trim(), attr(6).trim(), attr(7).trim(), attr(8).trim(), attr(9).trim())
      }
    ).toDF()


    val ratingRDD = spark.sparkContext.textFile(ratingDataPath)

    val ratingDF: DataFrame = ratingRDD.map(
      line => {
        val attr: Array[String] = line.split(",")
        Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      }
    ).toDF()

    val tagRDD = spark.sparkContext.textFile(tagsDataPath);

    val tagDF: DataFrame = tagRDD.map(
      line => {
        val attr: Array[String] = line.split(",")
        Tag(attr(0).toInt, attr(1).toInt, attr(2), attr(3).toInt)
      }
    ).toDF()

    //todo 数据预处理 : 将movie对应的tag信息添加进去 ， 加一列数据 tag1|tag2|tag3

    /**
     * 数据格式
     * mid , tags
     *
     * tags:tag1|tag2
     */
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")

    //对newTag和movie做一个join操作，将数据合并在一起
    //左外连接
    val movieWithTags: DataFrame = movieDF.join(newTag, Seq("mid"), "left")

    //将数据保存到mysql中
    //    storeDataToMysql(movieDF, ratingDF, tagDF);

    //保存数据到es
    storeDataToES(movieWithTags);

    spark.stop();
  }

  //保存数据到Mysql
  def storeDataToMysql(MovieDF: DataFrame, RatingDF: DataFrame, tagDF: DataFrame) = {
    //    连接数据库的相关配置
    val prop: Properties = new Properties()
    prop.put("driver", config("mysql.driver"))
    prop.put("user", config("mysql.user"))
    prop.put("password", config("mysql.password"))
    //    将dataframe写入到mysql
    MovieDF.write.jdbc(config("mysql.url"), Mysql_MovieTable_Name, prop)

    RatingDF.write.jdbc(config("mysql.url"), Mysql_RatingTable_Name, prop)

    tagDF.write.jdbc(config("mysql.url"), Mysql_TagTable_Name, prop)
  }

  //保存数据到ElasticSearch
  def storeDataToES(movieWithTags: DataFrame) = {

    val client: RestHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200)))
    val request: IndexRequest = new IndexRequest()

    request.index("recommender")
    val indexRequest = new GetIndexRequest("recommender")

    //判断索引是否存在
    val exist: Boolean = client.indices().exists(indexRequest, RequestOptions.DEFAULT)

    //如果索引存在
    if (exist) {
      val deleteIndexRequest = new DeleteIndexRequest()
      deleteIndexRequest.indices("recommender")
      //删除索引
      client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);

    } else {
      //否则
      //创建索引
      client.indices().create(new CreateIndexRequest("recommender"), RequestOptions.DEFAULT);
    }

    //ES的相关配置
    val options = Map(
      //如果没有index,则新建
      "es.index.auto.create" -> "true",
      "es.nodes.wan.only" -> "true",
      //安装es的服务器节点
      "es.nodes" -> "localhost",
      //es的端口
      "es.port" -> "9200",
      "es.mapping.id" -> "mid"
    )

    movieWithTags.write.format("org.elasticsearch.spark.sql").options(options).mode("overwrite")
      .save("recommender" + "/" + ES_MOVIE_INDEX)
  }
}

//定义一些样例类

/**
 * movie数据集
 * 260                                            电影id:mid
 * ^Star Wars: Episode IV - A New Hope (1977)     电影名称:name
 * ^Princess Leia is captured and held            详情描述:descri
 * ^121minutes                                    时长:timelog
 * ^September 21, 2004                            发行时间:issue
 * ^1977                                          拍摄时间:shoot
 * ^English                                       语言:language
 * ^Action|Adventure|Sci-Fi                       类型:genres
 * ^Mark Hamill|Harrison Ford                     演员表:actors
 * ^George Lucas                                  导演:directors
 * */
case class Movie(mid: Int, name: String, descri: String, timelog: String, issue: String, shoot: String, language: String,
                 genres: String, actors: String, directors: String)


/*
* 1,2968,1.0,1260759200  uid:用户id ， 电影id:mid , 评分:score , 评分时间:timestamp
* */


case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)


/**
 * 15,1955,dentist,1193435061    uid:用户id ， 电影id:mid , 标签:tag , 评分时间:timestamp
 */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)