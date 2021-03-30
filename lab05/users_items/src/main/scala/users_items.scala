import connectors.{HDFSConnect, Visit}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object users_items extends App {
  val conf = new SparkConf(true).setAppName("irina.samsonova").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate

  spark.conf.set("spark.sql.shuffle.partitions", 800)

  val hdfs = new HDFSConnect(spark)

  import spark.implicits._

  main

  def main: Unit = {
    val df = hdfs.read
    val format = new java.text.SimpleDateFormat("yyyyMMdd")
    val lastDate = format.format(df.selectExpr("max(timestamp)").as[Long].collect()(0))
    val res = groupVisits(df)
    hdfs.write(res, lastDate)
  }

  private def groupVisits(data: Dataset[Visit]): DataFrame = {
    //    data
    //      .withColumn("code", concat(
    //        col("event_type"),
    //        lit("_"),
    //        lower(regexp_replace(col("item_id"), "-", "_")))
    //      )
    //      .orderBy("uid", "code", "date")
    //      .groupBy("uid", "code")
    //      .agg(last(col("date")).as("p_date"))
    //      .groupBy("uid", "p_date")
    //      .pivot("code")
    //      .agg(count("*").as("count"))
    //      .na.fill(0)

    data
      .withColumn("type_item_id", concat(
        col("event_type"),
        lit("_"),
        lower(regexp_replace(col("item_id"), "-", "_")))
      )
      .groupBy(col("uid"))
      .pivot("type_item_id")
      .agg(count("*"))
      .na.fill(0)
  }
}
