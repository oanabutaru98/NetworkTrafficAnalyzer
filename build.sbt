name := "spark_kafka"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.4.0",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.4.0",
  ("com.datastax.spark" %% "spark-cassandra-connector" % "2.5.0").exclude("io.netty", "netty-handler"),
  ("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0").exclude("io.netty", "netty-handler")
)

