package com.example.hudi

import com.typesafe.scalalogging.LazyLogging
import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL}
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

object HudiWriteChecker extends App with LazyLogging {

  private[hudi] lazy val basePath: String = System.getProperty("sandbox.path")

  private[hudi] lazy val sparkCores: String = System.getProperty("spark.cores")

  private[hudi] lazy val sparkDriverMemory: String = System.getProperty("spark.memory")

  private[hudi] lazy val hudiTables: Int = System.getProperty("hudi.tables").toInt

  private[hudi] def tablePath(tableName: String) = s"$basePath/$tableName"

  private[this] val sparkConf: SparkConf = new SparkConf()
    .setMaster(s"local[${sparkCores}]")
    .setAppName("Hudi Compaction Issue 5298")
    .set("spark.driver.memory", sparkDriverMemory)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  private[this] val sparkSession: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate
  private[this] val nThreads = hudiTables
  private[this] val delay = 5000

  implicit val ec: ExecutionContext = new ExecutionContext {
    val threadPool: ExecutorService = Executors.newFixedThreadPool(nThreads)

    def execute(runnable: Runnable): Unit = {
      threadPool.submit(runnable)
    }

    def reportFailure(t: Throwable): Unit = {
      logger.error("Hudi error: ", t)
      System.exit(-1)
    }
  }

  // Create concurrent writers, but each is working with the same thread safe spark session
  private val writeTasks = for (i <- 1 to nThreads) yield Future {
    val writer = TableWriter(sparkSession, s"test_table_$i")
    val reader = TableReader(sparkSession, s"test_table_$i")
    while (true) {
      writer.write()
      // we cannot reproduce it on a snapshot
      //reader.readSnapshot()
      // reader fails with incremental mode only
      reader.readIncremental()
      Thread.sleep(delay)
    }
  }

  Await.ready(Future.firstCompletedOf(writeTasks), Duration.Inf)

}

object TableWriter {
  def apply(sparkSession: SparkSession, tableName: String): TableWriter = new TableWriter(sparkSession, tableName)
}

final class TableWriter private(spark: SparkSession, tableName: String) extends LazyLogging {

  val max_records_per_commit = 200
  val partitions_count = 5

  def write(): Unit = {
    try {
      val df = createRandomData()
      writeToHudi(df)
    } catch {
      case t: Throwable => logger.error("Writer failure: ", t); System.exit(-1)
    }
  }

  private def createRandomData(): DataFrame = {
    val recordsCount = max_records_per_commit
    // Data
    val data: Seq[Row] =
      for (id <- 1 to recordsCount)
        yield {
          Row(
            id.toLong, // sequential ID to trigger delta file creation
            System.currentTimeMillis(), // new timestamp for pre-combine
            "partition_" + id / (max_records_per_commit / (partitions_count - 1)),
            Row(
              Row(
                "root_account_id_" + Random.nextInt(10) // random value
              ),
              "U"
            )
          )
        }
    // Schema
    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("lsn", LongType, nullable = false),
        StructField("partition", StringType, nullable = false),
        StructField(
          "value",
          StructType(
            Seq(
              StructField(
                "after",
                StructType(
                  Seq(
                    StructField("root_account_id", StringType, nullable = false)
                  )
                ),
                nullable = false
              ),
              StructField("op", StringType, nullable = false)
            )
          ),
          nullable = true
        )
      )
    )
    val rows = spark.sparkContext.parallelize(data, 4)
    val df = spark
      .createDataFrame(rows, schema)
      .withColumn("upsert_root_account_ids", typedLit[Seq[Long]](List()))
      .withColumn("delete_root_account_ids", typedLit[Seq[Long]](List()))
    df.orderBy()
  }

  private def writeToHudi(df: DataFrame): Unit = {
    df.write
      .format("hudi")
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "lsn")
      .option(PARTITIONPATH_FIELD.key(), "partition")
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")

      .option("hoodie.finalize.write.parallelism", "4")
      .option("hoodie.upsert.shuffle.parallelism", "4")

      .option("hoodie.compact.inline", "true")
      .option("hoodie.datasource.compaction.async.enable", "false")

      .option("hoodie.compact.inline.max.delta.commits", "2")
      .option("hoodie.compact.inline.trigger.strategy", "NUM_COMMITS")
      //disable cleaning as it is not the root cause
      .option("hoodie.clean.automatic", "false")
      .option("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS")
      .option("hoodie.cleaner.commits.retained", "18")
      .option("hoodie.metadata.cleaner.commits.retained", "18")
      .option("hoodie.keep.min.commits", "36")
      .option("hoodie.keep.max.commits", "72")
      //disable clustering
      .option("hoodie.clustering.inline", "false")
      //do not use metadata table
      .option("hoodie.metadata.enable", "false")
      //explicitly specify the marker
      .option("hoodie.write.markers.type", "DIRECT")
      .option("hoodie.embed.timeline.server", "false")
      .option("hoodie.index.type", "BLOOM")
      .option("hoodie.bloom.index.update.partition.path", "true")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
      .option("hoodie.cleaner.policy.failed.writes", "EAGER")
      .option("hoodie.write.concurrency.mode", "single_writer")
      // on EMR cluster with multiple StreamQuery(s) optimistic concurrency model has been tried with zookeeper as a lock provider
      // However, since each streaming query (and in this case each thread writes to its own table) this ahd no effect
      //.option("hoodie.write.concurrency.mode", "OPTIMISTIC_CONCURRENCY_CONTROL")
      //.option("hoodie.cleaner.policy.failed.writes", "LAZY")
      .mode(SaveMode.Append)
      .save(HudiWriteChecker.tablePath(tableName))
  }

}

object TableReader {
  def apply(session: SparkSession, tableName: String): TableReader = new TableReader(session, tableName)
}

final class TableReader(spark: SparkSession, tableName: String) extends LazyLogging {

  def readSnapshot(): Unit = {
    val tablePath = HudiWriteChecker.basePath + "/" + tableName
    println(s"Reading from $tableName in thread ${Thread.currentThread().getName}:")
    try {
      spark.read
        .format("hudi")
        .load(tablePath)
        .show()
    } catch {
      case t: Throwable => logger.error("Reader failure (snapshot): ", t); System.exit(-1)
    }
  }

  def readIncremental(): Unit = {
    val tablePath = HudiWriteChecker.basePath + "/" + tableName
    println(s"Reading from $tableName in thread ${Thread.currentThread().getName}:")
    try {
      spark.read
        .format("hudi")
        .option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(BEGIN_INSTANTTIME.key(), "0")
        .load(tablePath)
        .show()
    } catch {
      case t: Throwable => logger.error("Reader failure (incremental): ", t); System.exit(-1)
    }
  }

}