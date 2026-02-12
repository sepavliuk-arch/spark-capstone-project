package com.sepavliuk.capstone

import cats.effect.{ExitCode, IO, IOApp}
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import pureconfig.ConfigReader

import scala.reflect.ClassTag

/**
 * Base class for all Spark Jobs.
 * @param appName The name of the Spark application
 * @tparam C The type of the Config case class
 */
abstract class BaseEntryPoint[C: ConfigReader : ClassTag] extends IOApp {

  protected def sparkOptions: Map[String, String] = Map.empty
  protected var _cliArgs: List[String] = List.empty

  protected def isLocalRun(config: C): Boolean
  protected def appName(config: C): String

  protected def compute(spark: SparkSession, config: C): IO[Unit]

  override def run(args: List[String]): IO[ExitCode] = {
    _cliArgs = args

    for {
      config <- IO.delay(ConfigSource.default.loadOrThrow[C])

      _ <- SparkUtils.createSparkSession(
        appName(config),
        isLocalRun(config),
        sparkOptions
      ).use { spark =>
        compute(spark, config)
      }
    } yield ExitCode.Success
  }
}