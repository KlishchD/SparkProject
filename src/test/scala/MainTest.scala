import org.scalatest.FunSuite
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class MainTest extends FunSuite {

  test("getFirstWave") {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val retweets = Seq((0, 1, 0), (1, 2, 1), (0, 2, 0), (2, 3, 2), (2, 10, 1), (2, 11, 0), (1, 12, 0), (12, 13, 0))
      .toDF("user_id", "subscriber_id", "message_id")
      .as[Main.Retweet]
    val messages = Seq((0, 0), (1, 1), (2, 2)).toDF("user_id", "message_id").as[Main.Message]
    val expected = Seq(
      Main.Retweet(0, 1, 0),
      Main.Retweet(1, 2, 1),
      Main.Retweet(2, 3, 2),
      Main.Retweet(0, 2, 0)
    )
    val actual = Main.getFirstWave(retweets, messages, spark)

    assert(actual.collect().toSet == expected.toSet)
  }
  test("getSecondWave") {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val retweets = Seq((0, 1, 0), (1, 2, 1), (0, 2, 0), (2, 3, 2), (2, 10, 1), (2, 11, 0), (1, 12, 0), (12, 13, 0))
      .toDF("user_id", "subscriber_id", "message_id")
      .as[Main.Retweet]
    val messages = Seq((0, 0), (1, 1), (2, 2)).toDF("user_id", "message_id").as[Main.Message]
    val expected = Seq(
      Main.Retweet(1, 10, 1),
      Main.Retweet(0, 11, 0),
      Main.Retweet(0, 12, 0)
    )
    val actual = Main.getSecondWave(retweets, messages, spark)

    assert(actual.collect().toSet == expected.toSet)
  }
  test("getTop") {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val retweets = Seq((1, 10, 1), (0, 11, 0), (0, 12, 0))
      .toDF("user_id", "subscriber_id", "message_id")
      .as[Main.Retweet]
    val usersDirs = Seq((0, "A", "B"), (1, "C", "D"))
      .toDF("user_id", "first_name", "last_name")
      .as[Main.User_dir]
    val messagesDirs = Seq((0, "Day"), (1, "Night"))
      .toDF("message_id", "text")
      .as[Main.Message_dir]

    val expected = Seq(Main.TopUser(0, "A", "B", 0, "Day", 2), Main.TopUser(1, "C", "D", 1, "Night", 1))

    val actual = Main.getTop(retweets, usersDirs, messagesDirs, spark)

    assert(actual.collect().toSet == expected.toSet)
  }

}
