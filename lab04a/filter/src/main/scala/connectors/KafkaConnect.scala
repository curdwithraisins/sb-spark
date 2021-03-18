package connectors

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Visit(
  event_type: String,
  category: String,
  item_id: String,
  item_price: String,
  uid: String,
  timestamp: Long
)

class KafkaConnect(spark: SparkSession) {
  import spark.implicits._

  val offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
  val topic_name: String = spark.sparkContext.getConf.get("spark.filter.topic_name")

  def read: Dataset[Visit] = {
    val schema = ScalaReflection.schemaFor[Visit].dataType.asInstanceOf[StructType]

    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", topic_name)
      .option("startingOffsets", offset)
      .option("endingOffsets", "latest")
      .load()
      .selectExpr("cast (value as string) as json")
      .select(from_json($"json", schema).as[Visit])
  }
}
