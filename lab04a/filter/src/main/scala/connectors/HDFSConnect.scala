package connectors

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class HDFSConnect(spark: SparkSession) {
  private val output_dir_prefix = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")

  def write(df: DataFrame, visitType: String) = {
    df
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("p_date")
      .json(s"$output_dir_prefix/$visitType/")
  }
}
