package com.sepavliuk.capstone

import munit.CatsEffectSuite
import org.apache.spark.sql.SparkSession

trait MUnitSharedSparkSession { self: CatsEffectSuite =>

  private var _spark: SparkSession = _

  implicit def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    _spark = SparkSession.builder()
      .appName("MUnitSparkSession")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (_spark != null) {
      _spark.stop()
    }
  }
}