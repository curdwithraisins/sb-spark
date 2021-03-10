package connectors

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class Domain(
                   domain: String,
                   category: String
                 )

class PostgresConnect(spark: SparkSession) {
  import spark.implicits._

  def read = {
    spark.sqlContext.read.format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url", "jdbc:postgresql://10.0.0.5:5432/labdata")
      .option("dbtable","domain_cats")
      .option("user", "irina_samsonova")
      .option("password", "k0tXpcK4")
      .load()
      .as[Domain]
  }

  def write(data: DataFrame) = {
    data.write.format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("driver","org.postgresql.Driver")
      .option("url", "jdbc:postgresql://10.0.0.5:5432/irina_samsonova")
      .option("dbtable", "clients")
      .option("user", "irina_samsonova")
      .option("password", "k0tXpcK4")
      .save()
  }
}
