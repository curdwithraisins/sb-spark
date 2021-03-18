import connectors.{HDFSConnect, KafkaConnect, Visit}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object filter extends App {
  val conf = new SparkConf(true).setAppName("irina.samsonova").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate

  spark.conf.set("spark.sql.shuffle.partitions", 4000)

  main

  def main: Unit = {
    val df = new KafkaConnect(spark).read.persist

    val views = filterVisits(df, "view")
    val buys = filterVisits(df, "buy")

    new HDFSConnect(spark).write(views, "view")
    new HDFSConnect(spark).write(buys, "buy")
  }

  private def filterVisits(data: Dataset[Visit], visitType: String): DataFrame = {
    val getData = udf { (timestamp: Long) => new java.text.SimpleDateFormat("yyyyMMdd").format(timestamp) }

    data
      .filter(col("event_type").equalTo(visitType))
      .withColumn("date", getData(col("timestamp")))
      .withColumn("p_date", col("date"))
  }
}
