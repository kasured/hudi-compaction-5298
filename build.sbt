name := "hudi-compaction"

version := "0.1"

scalaVersion := "2.12.11"

val Spark = "3.1.2"
val Hudi = "0.9.0"
val Logging = "3.9.4"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Logging,
  "org.apache.hudi" %% "hudi-spark3-bundle" % Hudi,
  "org.apache.spark" %% "spark-sql" % Spark,
  "org.apache.spark" %% "spark-core" % Spark,
  "org.apache.spark" %% "spark-hive" % Spark
)

Compile / run / mainClass := Some("com.example.hudi.HudiWriteChecker")
