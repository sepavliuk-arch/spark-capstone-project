package com.sepavliuk.capstone.spark

import cats.effect.{ExitCode, IO}
import com.sepavliuk.capstone.BaseEntryPoint
import com.sepavliuk.capstone.config.Config
import com.sepavliuk.capstone.logging.Log
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.MDC
import pureconfig.generic.auto._


class OlapCubeGenerationJob(localDate: String)(implicit spark: SparkSession) {
  private val logger: Log = Log(getClass)

  def readData(spark: SparkSession): DataFrame = {
    val data = spark.read.parquet(s"output/sessions/localDate=${localDate}")
    data.printSchema()
    data
  }

  def processJob(): IO[Unit] = {
    val jobWithContext = for {
      _ <- logger.info(s"Starting processing ")

      data = readData(spark)
      _ <- logger.info(s"In a middle of processing")

      cubed = cubeData(data)
      prepared = prepareForSink(cubed)

      _ <- logger.info(s"Write to output")

     _ <- IO.blocking(sink(prepared))

      _ <- logger.info(s"job completed successfully")
    } yield ()

    jobWithContext.guarantee(IO.delay(MDC.clear()))
  }

  def cubeData(df: DataFrame): DataFrame = {
    df.cube("browser", "platform").agg(
      grouping("browser").as("browser_grp"),
      grouping("platform").as("platform_grp"),
      sum("totalClicks").as("metric_clicks"),
      sum("totalViews").as("metric_views"),
      sum("totalPurchases").as("metric_purchases"),
      count_distinct(col("userId")).as("metric_unique_users")
    )
      .withColumn("dim_browser", when(col("browser_grp") === 1, "ALL").otherwise(col("browser")))
      .withColumn("dim_platform", when(col("platform_grp") === 1, "ALL").otherwise(col("platform")))
  }

  def prepareForSink(df: DataFrame): DataFrame = {

    df.select(
      col("dim_browser"),
      col("dim_platform"),
      col("metric_clicks"),
      col("metric_views"),
      col("metric_purchases"),
      col("metric_unique_users"),
      current_date().as("localDate")
    )
  }

  def sink(df: DataFrame): Unit = {
    df.
      write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .save("output/olap_cube/")
  }


}

object OlapCubeGenerationJob extends BaseEntryPoint[Config]{
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
      result <- new OlapCubeGenerationJob(date)(spark).processJob()
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
