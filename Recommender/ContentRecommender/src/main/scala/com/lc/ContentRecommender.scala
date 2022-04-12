package com.lc

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.jblas.DoubleMatrix

import java.util.Properties

object ContentRecommender {
  //定义常量
  val Mysql_Movie_Collection = "movie"
  val Content_Movie_Recs = "contentmovierecs"

  //定义配置项
  val config = Map(
    "spark.cores" -> "local[*]",
    "mysql.url" -> "jdbc:mysql://localhost:3306/recommendsystem",
    "mysql.driver" -> "com.mysql.jdbc.Driver",
    "mysql.user" -> "root",
    "mysql.password" -> "000000",
  )

  //数据库连接配置项
  val prop = new Properties()
  prop.put("driver", config("mysql.driver"))
  prop.put("user", config("mysql.user"))
  prop.put("password", config("mysql.password"))

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster(config("spark.cores"))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import org.apache.spark.ml.feature.IDF
    import spark.implicits._
    //加载数据，并且做数据预处理
    val movieTagsDF: DataFrame = spark.read.jdbc(config("mysql.url"), Mysql_Movie_Collection, prop).rdd.map(
      line => {
        (line.getAs[Int]("mid"), line.getAs[String]("name"), line.getAs[String]("genres").map(c => if (c == '|') ' ' else c))
      }
    ).toDF("mid", "name", "genres").cache()

    //核心部分:使用TF-IDF从内容信息中提取电影特征向量
    //实例化一个分词器，默认按照空格分词
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")

    //使用分词器对原始数据做转换，生成新的一列words
    val wordsData: DataFrame = tokenizer.transform(movieTagsDF)

    //引入HashingTF工具，将一个词语序列转化成对应的词频
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)

    val featurizedData: DataFrame = hashingTF.transform(wordsData)

    //truncate代表不压缩结果可以完全展示出来
    //    featurizedData.show(100,truncate = false)

    //引入IDF，可以得到IDF模型
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    //训练IDF模型得到每个词的逆文档频率
    val idfModel: IDFModel = idf.fit(featurizedData)

    //使用模型对源数据进行处理，得到文档中每个词的TFIDF值，作为新的特征向量
    val rescaleData: DataFrame = idfModel.transform(featurizedData)

    //    rescaleData.show(1000, truncate = false)

    val movieFeatures: RDD[(Int, DoubleMatrix)] = rescaleData.map(
      //SparseVector稀疏向量
      line => (line.getAs[Int]("mid"), line.getAs[SparseVector]("features").toArray)
    ).rdd.map(
      x => (x._1, new DoubleMatrix(x._2))
    )

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
    movieRecs.write.mode(SaveMode.Overwrite).jdbc(config("mysql.url"), Content_Movie_Recs, prop)
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

//定义样例类
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

//定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)

//定义基于预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: String)

//定义基于电影内容信息提取出的特征向量的电影相似度列表(相似度矩阵)
case class MovieRecs(mid: Int, recs: String)