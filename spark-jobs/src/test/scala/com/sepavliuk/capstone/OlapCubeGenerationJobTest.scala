package com.sepavliuk.capstone

import com.sepavliuk.capstone.spark.OlapCubeGenerationJob
import munit.CatsEffectSuite
import org.apache.spark.sql.functions.col

class OlapCubeGenerationJobTest extends CatsEffectSuite with MUnitSharedSparkSession {

  test("cubeData should generate all combinations and rename nulls to ALL") {
    val sparkSession = spark
    import sparkSession.implicits._
    val job = new OlapCubeGenerationJob("2026-02-09")

    val inputDf = Seq(
      ("u1", "s1", 100.0, 2L, 10L, 1L, "Chrome", "Web"),
      ("u2", "s2", 50.0,  0L, 5L,  0L, "Safari", "iOS")
    ).toDF("userId", "sessionId", "sessionDuration", "totalClicks", "totalViews", "totalPurchases", "browser", "platform")

    val result = job.cubeData(inputDf)

    val grandTotal = result.filter(col("dim_browser") === "ALL" && col("dim_platform") === "ALL")
    assertEquals(grandTotal.count(), 1L)
    assertEquals(grandTotal.select("metric_views").collect().head.getAs[Long](0), 15L)
    assertEquals(grandTotal.select("metric_unique_users").collect().head.getAs[Long](0), 2L)

    val chromeTotal = result.filter(col("dim_browser") === "Chrome" && col("dim_platform") === "ALL")
    assertEquals(chromeTotal.select("metric_clicks").collect().head.getAs[Long](0), 2L)

    val iosTotal = result.filter(col("dim_browser") === "ALL" && col("dim_platform") === "iOS")
    assertEquals(iosTotal.select("metric_views").collect().head.getAs[Long](0), 5L)
  }

  test("prepareForSink should select the correct columns and include localDate") {
    val sparkSession = spark
    import sparkSession.implicits._
    val job = new OlapCubeGenerationJob("2026-02-09")

    val dummyCube = Seq(
      ("Chrome", "Web", 10L, 100L, 1L, 1L)
    ).toDF("dim_browser", "dim_platform", "metric_clicks", "metric_views", "metric_purchases", "metric_unique_users")

    val prepared = job.prepareForSink(dummyCube)

    val expectedCols = Set("dim_browser", "dim_platform", "metric_clicks", "metric_views", "metric_purchases", "metric_unique_users", "localDate")
    assertEquals(prepared.columns.toSet, expectedCols)
  }
}
