package com.sepavliuk.capstone.spark

import cats.effect.{ExitCode, IO}
import com.sepavliuk.capstone.BaseEntryPoint
import com.sepavliuk.capstone.config.Config
import com.sepavliuk.capstone.logging.Log
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.MDC
import pureconfig.generic.auto._


class AggregateSession(utcDate: String)(implicit spark: SparkSession) {
  private val logger: Log = Log(getClass)

  def readData(spark: SparkSession): DataFrame = {
    val data = spark.read.parquet(s"output/sessionized/utcDate=${utcDate}")
    data.printSchema()
    data
  }

  def processJob(): IO[Unit] = {
    val jobWithContext = for {
      _ <- logger.info(s"Starting processing ")
      data = readData(spark)
      _ <- logger.info(s"In a middle of processing")

      statistic = countStatistic(data)
      sessionDuration = getSessionDuration(data)
      wholeDf = joinData(statistic, sessionDuration)
      prepared = prepareForSink(wholeDf)

      _ <- logger.info(s"Write to output")

     _ <- IO.blocking(sink(prepared))

      _ <- logger.info(s"Join job completed successfully")
    } yield ()

    jobWithContext.guarantee(IO.delay(MDC.clear()))
  }

  def countStatistic(df: DataFrame): DataFrame = {
    df
      .groupBy("sessionId", "userId", "platform", "browser")
      .pivot("eventType", List("view", "click", "purchase"))
      .count()
      .na.fill(0)
      .withColumnRenamed("view", "totalViews")
      .withColumnRenamed("click", "totalClicks")
      .withColumnRenamed("purchase", "totalPurchases")
  }

  def getSessionDuration(df: DataFrame): DataFrame = {
    df
      .filter(col("timestamp").isNotNull)
      .groupBy("sessionId", "userId")
      .agg(
        min(col("timestamp").cast("timestamp").cast("double")).as("min_ts"),
        max(col("timestamp").cast("timestamp").cast("double")).as("max_ts")
      )
      .withColumn("sessionDuration", col("max_ts") - col("min_ts"))
  }

  def joinData(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.join(df2, List("sessionId", "userId"), "inner")
  }

  def prepareForSink(df: DataFrame): DataFrame = {
    df.select(
      col("userId"),
      col("sessionId"),
      col("sessionDuration"),
      col("totalClicks"),
      col("totalViews"),
      col("totalPurchases"),
      col("browser"),
      col("platform"),
      current_date().as("localDate")
    )
  }

  def sink(df: DataFrame): Unit = {
    df.
      write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("localDate")
      .save("output/sessions/")
  }


}

object AggregateSession extends BaseEntryPoint[Config]{
  private val logger: Log = Log(getClass)

  override protected def isLocalRun(config: Config): Boolean = config.phoenix.localRun

  override protected def appName(config: Config): String = config.phoenix.jobName

  override protected def compute(spark: SparkSession, config: Config): IO[Unit] =
    for {
      spark <- IO.delay(spark)
      _ <- logger.info(s"Starting Job")
      date <- IO.fromOption(_cliArgs.headOption)(
        new IllegalArgumentException("Date can't be empty")
      )
      _ <- logger.info(s"date ${date}")

      result <- new AggregateSession(date)(spark).processJob()
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
