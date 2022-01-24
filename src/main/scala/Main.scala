import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Main {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("MostPopular")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val usersDirs = load("src/main/resources/user_dirs.avro", spark).as[User_dir]
    val messageDirs = load("src/main/resources/messages_dirs.avro", spark).as[Message_dir]
    val messages = load("src/main/resources/messages.avro", spark).as[Message].cache()
    val retweet = load("src/main/resources/retweets.avro", spark).as[Retweet].distinct().cache()

    val firstWave = getFirstWave(retweet, messages, spark)
    val topFirstWave = getTop(firstWave, usersDirs, messageDirs, spark)

    topFirstWave.show(10, truncate = false)

    val secondWave = getSecondWave(retweet, messages, spark)
    val topSecondWave = getTop(secondWave, usersDirs, messageDirs, spark)

    topSecondWave.show(10, truncate = false)

    spark.close()
  }

  /**
   * Method for loading data from a file in Avro format
   * @param path String that defines filepath to the file we want to load
   * @return returns DataFrame with data from provided file
   */
  def load(path: String, spark: SparkSession): DataFrame = {
    spark.read
      .format("avro")
      .load(path)
  }

  /**
   * Method for selecting first wave of retweets from all retweets
   * @param retweets Dataset[Retweet] that contains data about all waves of retweets
   * @param messages Dataset[Messages] that contains data about messages' author
   * @return Dataset[Retweet] that contains data about first wave of retweets
   */
  def getFirstWave(retweets: Dataset[Retweet], messages: Dataset[Message], spark: SparkSession): Dataset[Retweet] = {
    import spark.implicits._
    retweets
      .join(messages, Array("user_id", "message_id"))
      .as[Retweet]
  }

  /**
   * Method for selecting second wave of retweets from all retweets
   * @param retweets Dataset[Retweet] that contains data about all waves of retweets
   * @param messages Dataset[Messages] that contains data about messages' author
   * @return Dataset[Retweet] that contains data about second wave of retweets
   */
  def getSecondWave(retweets: Dataset[Retweet], messages: Dataset[Message], spark: SparkSession): Dataset[Retweet] = {
    import spark.implicits._
    val subscribers = retweets.select($"user_id".as("user"), $"subscriber_id".as("subscriber")).distinct()

    retweets.join(messages.withColumnRenamed("user_id", "author_id"), "message_id")
      .filter($"user_id" =!= $"author_id")
      .join(subscribers, $"user_id" === $"subscriber")
      .filter($"user" === $"author_id")
      .select($"user".as("user_id"), $"subscriber_id", $"message_id")
      .as[Retweet]
  }

  /**
   * Gets list of top users by retweets in a wave by aggregating waveRetweets and joining result with usersDirs and messageDirs to add more details about user and message
   * @param waveRetweets Dataset[Retweet] that contains data about retweets in a wave
   * @param usersDirs Dataset[User_dir] that contains data about users
   * @param messageDirs Dataset[Message_dir] that contains data about messages
   * @return Dataset[TopUser] that contains detailed information about retweets
   */
  def getTop(waveRetweets: Dataset[Retweet], usersDirs: Dataset[User_dir], messageDirs: Dataset[Message_dir], spark: SparkSession): Dataset[TopUser] = {
    import spark.implicits._
    waveRetweets.groupBy("user_id", "message_id")
      .agg(count("user_id").as("number_retweets"))
      .join(usersDirs, "user_id")
      .join(messageDirs, "message_id")
      .select("user_id", "first_name", "last_name", "message_id", "text", "number_retweets")
      .sort($"number_retweets".desc)
      .as[TopUser]
  }

  case class User_dir(user_id: Int, first_name: String, last_name: String)

  case class Message_dir(message_id: Int, text: String)

  case class Message(user_id: Int, message_id: Int)

  case class Retweet(user_id: Int, subscriber_id: Int, message_id: Int)

  case class Subscriber(user: Int, subscriber: Int)

  case class TopUser(user_id: Int, first_name: String, last_name: String, message_id: Int, text: String, number_retweets: BigInt)
}
