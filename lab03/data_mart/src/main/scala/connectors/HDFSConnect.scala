package connectors

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.net.URL
import java.net.URLDecoder.decode
import scala.util.Try

case class Weblogs(uid: String, timestamp: String, url: String)

class HDFSConnect(spark: SparkSession) {
  import spark.implicits._

  def read = {
    val decode_url = udf { (url: String) => Try(new URL(decode(url)).getHost).toOption}

    spark.read.json("/labs/laba03/weblogs.json")
      .withColumn("data", explode($"visits"))
      .select("uid", "data.timestamp", "data.url")
      .withColumn("url", regexp_replace(decode_url(col("url")), "^www\\.", ""))
      .as[Weblogs]
  }
}
