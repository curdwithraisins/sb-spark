package connectors

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.util.Try

case class Visit(
                  event_type: String,
                  category: String,
                  item_id: String,
                  item_price: String,
                  uid: String,
                  timestamp: Long
                )

class HDFSConnect(spark: SparkSession) {
  import spark.implicits._

  private val input_dir = Try{spark.sparkContext.getConf.get("spark.users_items.input_dir")}.getOrElse("data/visits/*/*/*")
  private val output_dir = Try{spark.sparkContext.getConf.get("spark.users_items.output_dir")}.getOrElse("data/users_items/")
  private val update = Try{spark.sparkContext.getConf.get("spark.users_items.update")}.getOrElse(0)

  def read: Dataset[Visit] = {
    spark.read.json(input_dir).as[Visit]
  }

  def write(df: DataFrame) = {
    df.write
      .mode(if (update == 0) SaveMode.Overwrite else SaveMode.Append)
      .partitionBy("p_date")
      .parquet(s"$output_dir/")
  }
}
