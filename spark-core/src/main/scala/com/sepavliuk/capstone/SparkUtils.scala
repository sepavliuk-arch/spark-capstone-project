package com.sepavliuk.capstone

import cats.effect.{IO, Resource}
import org.apache.spark.sql.SparkSession

object SparkUtils {

  private def initSpark(appName: String, localRun: Boolean, additionalOptions: Map[String, String]): SparkSession = {
    val builder = SparkSession.builder()
      .appName(appName)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("com.amazonaws.services.s3.enableV4", "true")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

    additionalOptions.foreach { case (key, value) =>
      builder.config(key, value)
    }

    if (!additionalOptions.contains("spark.hadoop.fs.s3a.aws.credentials.provider")) {
      if (localRun) {
        builder
          .master("local[*]")
          .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
      } else {
        builder
          .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.WebIdentityTokenCredentialsProvider")
      }
    }

    builder.getOrCreate()
  }

  def createSparkSession(
                          appName: String,
                          localRun: Boolean,
                          additionalOptions: Map[String, String] = Map.empty
                        ): Resource[IO, SparkSession] = {
    Resource.make(IO {
      initSpark(appName, localRun, additionalOptions)
    })(spark => IO(spark.stop()))
  }
}