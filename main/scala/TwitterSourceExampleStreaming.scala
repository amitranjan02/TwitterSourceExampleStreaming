
//import TwitterSourceExampleStreaming.CountSentimentWithString
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{ColumnName, DataFrame, Dataset, ForeachWriter, Row, SparkSession}
import org.structured_streaming_sources.twitter.TwitterStreamingSource

// Access Token: 1265298997826052097-hHMoN8siR5nVAAtS2TpY5SzRix8pFr
// Access Token Secret: Wl0Sh3QVpOK1orTwycJ6dRGkZxfDsDVqFqXZc1NsAkrep
//API Key: VFw9BuEyUNMlJVgUGnUuyOFwN
// API Secret Key: IaJNZbWpSsbC5TegXicbmKGDYClmmpnfMlYQQHOU3cqNoTiFS9
//https://github.com/hienluu/twitter-streaming/blob/master/src/main/scala/TwitterStreamingApp.scala

/*
 * Created by hluu on 3/11/18.
 */

object TwitterSourceExampleStreaming {
  private val SOURCE_PROVIDER_CLASS = {
    TwitterStreamingSource.getClass.getCanonicalName
  }

  def main(args: Array[String]): Unit = {
    println("TwitterSourceExample")

    val providerClassName = SOURCE_PROVIDER_CLASS.substring(0,SOURCE_PROVIDER_CLASS.indexOf("$"))
    println(providerClassName)

    if (args.length != 4) {
      println("Usage: <consumer key>, <consumer secret> <access token> <access token secret>")
      sys.exit(1)
    }

    val Array(consumerKey,consumerSecret,accessToken,accessTokenSecret) = args.take(4)

    // create a Spark session
    val spark = SparkSession
      .builder
      .appName("TwitterSourceExampleStreaming")
      .master("local[*]")
      .getOrCreate()

    //Check for spark version
    println("Spark version: " + spark.version)

    val stopWordsRDD = spark.sparkContext.textFile("src/main/resources/stop-words.txt")
    val posWordsRDD = spark.sparkContext.textFile("src/main/resources/pos-words.txt")
    val negWordsRDD = spark.sparkContext.textFile("src/main/resources/neg-words.txt")

    //To verify the Collect or reduce is failing due to streaming, I used below local file with tweets in it
    //val tweetsRDD = spark.sparkContext.textFile("src/main/resources/tweets.txt")

    val positiveWords = posWordsRDD.collect().toSet
    val negativeWords = negWordsRDD.collect().toSet
    val stopWords = stopWordsRDD.collect().toSet

    //Since we only have negative and positive words for English, we are only going to read in english tweet in our stream.
    val tweetsDF = spark.readStream
                        .format(providerClassName)
                        .option(TwitterStreamingSource.CONSUMER_KEY,consumerKey)
                        .option(TwitterStreamingSource.CONSUMER_SECRET,consumerSecret)
                        .option(TwitterStreamingSource.ACCESS_TOKEN,accessToken)
                        .option(TwitterStreamingSource.ACCESS_TOKEN_SECRET,accessTokenSecret)
                        .load()
                        .where("userLang == 'en'")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //Print Schema for tweets
    tweetsDF.printSchema()
    //use Tweet As Text

    val CountSentimentWithString = udf((tweet: String) => {
      //println("tweet: "+ tweet)
      val wordsOfTweetClean = tweet.split(" ").map(_.toLowerCase).filter(_.matches("[a-z]+")).dropWhile(word => stopWords.contains(word))
      val tweetWithMeaningfulWords = wordsOfTweetClean.filter(word => negativeWords.contains(word)).union(wordsOfTweetClean.filter(word => positiveWords.contains(word)))
      val score = tweetWithMeaningfulWords.map(row => (if (positiveWords.contains(row)) 1 else if (negativeWords.contains(row)) -1 else 0))
      //println("Score Sum: "+ score.sum.##)
      if (score.sum > 0) {
        "Positive"
      }
      else if (score.sum < 0) {
        "Negative"
      }
      else {
        "Neutral"
      }
    })
    spark.udf.register("CountSentimentWithString",CountSentimentWithString)
    import org.apache.spark.sql.functions.{col,udf}
    val sentimentDF = tweetsDF.select(CountSentimentWithString(col("text")).as("sentiment"), $"createdDate")
    // Assingment 4: Formatted Output is produced by this Stream
    val sentiment = sentimentDF.groupBy("sentiment").count()
    // Assingment 4: Formatted Output is produced by this Stream
    val tweetQSF = sentiment.writeStream.outputMode("complete").
                            format("console").
                            start()

    //Experiment on how it will show in rollwing window
    val groupByWindow = sentimentDF.groupBy(window($"createdDate", "10 seconds", "5 seconds"), $"Sentiment")
                                       .agg(count("Sentiment").as("Sentiment_count"))
    //Experiment on how it will show in rollwing window
    val tweetQS = groupByWindow.writeStream.outputMode("complete").
                         format("console").
                         start()

    tweetQS.awaitTermination()
    tweetQSF.awaitTermination()

    Thread.sleep(1000 * 10)
  }
}

