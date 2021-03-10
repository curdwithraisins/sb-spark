package connectors

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

case class Visits(
                   category: String,
                   event_type: String,
                   item_id: String,
                   item_price: BigInt,
                   timestamp: Timestamp,
                   uid: String
                 )

class ElasticsearchConnect(spark: SparkSession) {
  import spark.implicits._

  def read = {
    spark.read
      .format("es")
      .option("es.batch.write.refresh", false)
      .option("es.nodes", "10.0.0.5:9200")
      .option("es.net.http.auth.user", "irina.samsonova")
      .option("es.net.http.auth.pass", "k0tXpcK4")
      .load("visits")
      .as[Visits]
  }
}
