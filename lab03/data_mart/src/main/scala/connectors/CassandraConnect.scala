package connectors

import org.apache.spark.sql.SparkSession

case class Clients(
                    uid: String,
                    gender: String,
                    age: Int
                  )

class CassandraConnect(spark: SparkSession) {
  import spark.implicits._

  spark.conf.set("spark.cassandra.connection.host","10.0.0.5")
  spark.conf.set("spark.cassandra.connection.port","9042")

  def read = {
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "labdata")
      .option("table", "clients")
      .load()
      .as[Clients]
  }
}
