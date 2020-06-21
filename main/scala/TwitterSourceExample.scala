import org.apache.spark.sql.SparkSession
import org.structured_streaming_sources.twitter.TwitterStreamingSource
import org.apache.spark.sql.types.{DataType, TimestampType}

// Access Token: 1265298997826052097-hHMoN8siR5nVAAtS2TpY5SzRix8pFr
// Access Token Secret: Wl0Sh3QVpOK1orTwycJ6dRGkZxfDsDVqFqXZc1NsAkrep
//API Key: VFw9BuEyUNMlJVgUGnUuyOFwN
// API Secret Key: IaJNZbWpSsbC5TegXicbmKGDYClmmpnfMlYQQHOU3cqNoTiFS9
//https://github.com/hienluu/twitter-streaming/blob/master/src/main/scala/TwitterStreamingApp.scala

/*
 * Created by hluu on 3/11/18.
 */

object TwitterSourceExample {
  private val SOURCE_PROVIDER_CLASS = TwitterStreamingSource.getClass.getCanonicalName

  def computeWordScore(word: String, positiveWords: Set[String], negativeWords: Set[String]): Int =
    if (positiveWords.contains(word)) 1
    else if (negativeWords.contains(word)) -1
    else 0

  def main(args: Array[String]): Unit = {
    println("TwitterSourceExample")

    val providerClassName = SOURCE_PROVIDER_CLASS.substring(0, SOURCE_PROVIDER_CLASS.indexOf("$"))
    println(providerClassName)

    if (args.length != 4) {
      println("Usage: <consumer key>, <consumer secret> <access token> <access token secret>")
      sys.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    // create a Spark session
    val spark = SparkSession
      .builder
      .appName("TwitterStructuredStreaming")
      .master("local[*]")
      .getOrCreate()

    println("Spark version: " + spark.version)

    val stopWordsRDD = spark.sparkContext.textFile("src/main/resources/stop-words.txt")
    val posWordsRDD = spark.sparkContext.textFile("src/main/resources/pos-words.txt")
    val negWordsRDD = spark.sparkContext.textFile("src/main/resources/neg-words.txt")

    val positiveWords = posWordsRDD.collect().toSet
    val negativeWords = negWordsRDD.collect().toSet
    val stopWords = stopWordsRDD.collect().toSet


    val tweetDF = spark.readStream
                       .format(providerClassName)
                       .option(TwitterStreamingSource.CONSUMER_KEY, consumerKey)
                       .option(TwitterStreamingSource.CONSUMER_SECRET, consumerSecret)
                       .option(TwitterStreamingSource.ACCESS_TOKEN, accessToken)
                       .option(TwitterStreamingSource.ACCESS_TOKEN_SECRET, accessTokenSecret)
                       .load()
                       .where("userLang == 'en'")

    tweetDF.printSchema()
    //Old Code
    //val tweetQS = tweetDF.writeStream.format("console").option("truncate", false).start()


    //NewCode


    val tweetQS = tweetDF.writeStream.outputMode("append").queryName("en_tweet").format("memory").start()
    Thread.sleep(1000 * 60)
    tweetQS.stop()
    println("past wait time")
    val tweetQuery = spark.sql("select * from en_tweet")
    import org.apache.spark.sql.functions.udf
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val upper: String => String = _.toUpperCase
    val upperDF = udf(upper)
    spark.udf.register("myUpper", (input: String) => input.toUpperCase)
    spark.catalog.listFunctions().filter(("name like '%upper%'")).show(false)

    tweetQuery.withColumn("upper", upperDF(lit(tweetQuery.select("text")))).show
    println("past query code time")
    //Thread.sleep(1000 * 3500)
    tweetQS.awaitTermination()
  }
}