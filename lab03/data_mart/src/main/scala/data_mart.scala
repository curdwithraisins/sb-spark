import connectors.{CassandraConnect, Clients, Domain, ElasticsearchConnect, HDFSConnect, PostgresConnect, Visits, Weblogs}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object data_mart extends App {
  val conf = new SparkConf(true).setAppName("irina.samsonova").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate

  import spark.implicits._

  spark.conf.set("spark.sql.shuffle.partitions", 800)

  main

  def main: Unit = {
    val psql = new PostgresConnect(spark)

    val clients = new CassandraConnect(spark).read.repartition(col("uid"))
    val shopVisits = new ElasticsearchConnect(spark).read.repartition(col("uid"))
    val websitesVisits = new HDFSConnect(spark).read.repartition(col("uid"), col("url"))
    val domains = psql.read.repartition(col("domain"))

    val shopStats = getShopStats(shopVisits)
    val webStats = getWebStats(websitesVisits, domains)
    val stats = getAgeStats(clients)

      val res = stats
      .join(shopStats.alias("s"), stats.col("uid") === shopStats.col("uid"), "left_outer")
      .join(webStats.alias("w"), stats.col("uid") === webStats.col("uid"),"left_outer")
      .drop($"s.uid")
      .drop($"w.uid")

    psql.write(res)
  }

  private def addPrefix = udf {(prefix: String, category: String) => prefix + category}

  private def getAgeStats(clients: Dataset[Clients]) = {
    def age_cat = udf {(age: Int) => age match {
      case x if 18 until 25 contains x => "18-24"
      case x if 25 until 35 contains x => "25-34"
      case x if 35 until 45 contains x => "35-44"
      case x if 45 until 55 contains x => "45-54"
      case _ => ">=55"
    }}

    clients
      .withColumn("age_cat", age_cat(col("age")))
      .drop("age")
  }

  private def getShopStats(shopVisits: Dataset[Visits]) = {
    shopVisits
      .withColumn("category", addPrefix(lit("shop_"), regexp_replace(lower(col("category")), "-", "_")))
      .groupBy("uid")
      .pivot("category")
      .agg(count("*"))
  }

  private def getWebStats(websitesVisits: Dataset[Weblogs], domains: Dataset[Domain]) = {
    websitesVisits
      .join(domains, websitesVisits.col("url") === domains.col("domain"), "left_outer")
      .filter(col("category").isNotNull)
      .withColumn("category", addPrefix(lit("web_"), regexp_replace(lower(col("category")), "-", "_")))
      .groupBy("uid")
      .pivot("category")
      .agg(count("*"))
  }
}
