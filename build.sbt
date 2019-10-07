name := "kudu-test"

version := "0.1"

scalaVersion := "2.11.12"


val spark_version = "2.2.1"

libraryDependencies ++= Seq(
  // SPARK LIBRARIES
  "org.apache.spark" %% "spark-core" % spark_version ,
  "org.apache.spark" %% "spark-sql" % spark_version ,
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2",
  "org.apache.kudu" % "kudu-client" % "1.7.0",
   "org.apache.kudu" %% "kudu-spark2" % "1.7.0"
)