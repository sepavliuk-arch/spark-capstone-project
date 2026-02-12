package com.sepavliuk.capstone.spark

import cats.effect.{ExitCode, IO}
import com.sepavliuk.capstone.BaseEntryPoint
import com.sepavliuk.capstone.config.Config
import com.sepavliuk.capstone.logging.Log
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, hash, when, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.MDC
import pureconfig.generic.auto._




class Sessionization(implicit spark: SparkSession) {
  private val logger: Log = Log(getClass)

  def readData(spark: SparkSession): DataFrame = {
    val data = spark.read.option("multiLine", "true").json("input/")
    data.printSchema()
    data
  }

  def processJob(): IO[Unit] = {
    val jobWithContext = for {
      _ <- logger.info(s"Starting processing ")

      data = readData(spark)
      _ <- logger.info(s"In a middle of processing")

      processedData = splitOnSessions(data)
      withSession = addSessionId(processedData)
      enriched = enrich(withSession)
      prepared = prepareForSink(enriched)

      _ <- logger.info(s"Write to output")

     _ <- IO.blocking(sink(prepared))
      _ <- logger.info(s"job completed successfully")
    } yield ()

    jobWithContext.guarantee(IO.delay(MDC.clear()))
  }

  def splitOnSessions(df: DataFrame): DataFrame = {
    val typedTs = df.withColumn("ts", to_timestamp(col("timestamp")))

    val windowSpec = Window.partitionBy("userId").orderBy("ts")

    typedTs.withColumn("prevTs", lag("ts", 1).over(windowSpec))
      .withColumn("isNewSession", when(
        col("ts").cast("long") - col("prevTs").cast("long") > 1800 || col("prevTs").isNull || col("eventType") === "login", 1).
        otherwise(0))
      .withColumn("sessionIndex", sum("isNewSession").over(windowSpec))
  }

  def addSessionId(df: DataFrame): DataFrame = {
    def windowSpec = Window.partitionBy(col("userId"), col("sessionIndex"))
    df
      .withColumn("sessionStartTs", min("timestamp").over(windowSpec))
      .withColumn("sessionId", hash(concat(col("sessionStartTs"), col("userId"))))
  }

  def enrich(df: DataFrame): DataFrame = {
    df
      .withColumn("browser", regexp_extract(col("userAgent"), "(Chrome|Safari|Firefox|Edge)", 1))
      .withColumn("platform", regexp_extract(col("userAgent"), "\\(([^;]+);", 1))
  }

  def prepareForSink(df: DataFrame): DataFrame = {
    df.select(
    col("sessionId"),
    col("userId"),
    col("eventId"),
    col("ts").as("timestamp"),
    col("eventType"),
    col("platform"),
    col("browser"),
    col("isNewSession").cast("boolean"),
    to_date(to_utc_timestamp(current_timestamp(), "UTC")).alias("utc_date")
    )
  }

  def sink(df: DataFrame): Unit = {
    df.
      write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("utcDate")
      .save("output/sessionized/")
  }


}

object Sessionization extends BaseEntryPoint[Config]{
  private val logger: Log = Log(getClass)

  override protected def isLocalRun(config: Config): Boolean = config.phoenix.localRun

  override protected def appName(config: Config): String = config.phoenix.jobName

  override protected def compute(spark: SparkSession, config: Config): IO[Unit] =
    for {
      spark <- IO.delay(spark)

      _ <- logger.info(s"Starting Job")

      result <- new Sessionization()(spark).processJob()
        .guarantee(IO.delay {
          spark.stop()
          println("Spark stopped")
          MDC.clear()
        })
        .handleErrorWith(e =>
          logger.error(s"Job failed: ${e.getMessage}") *> IO.raiseError(e)
        ).as(ExitCode.Success)
    } yield result

}
