name := "data_mart"
version := "0.1"
scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.postgresql"  %% "postgresql" % "42.1.1",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.11.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2"
)

enablePlugins(JavaAppPackaging)