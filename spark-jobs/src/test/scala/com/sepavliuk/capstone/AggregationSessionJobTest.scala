package com.sepavliuk.capstone

import com.sepavliuk.capstone.spark.AggregateSession
import munit.CatsEffectSuite
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class AggregationSessionJobTest extends CatsEffectSuite with MUnitSharedSparkSession {

  private def createInputData() = {
    val sparkSession = spark
    import sparkSession.implicits._
    Seq(
      ("s1", "u1", "web", "chrome", "view", "2026-02-09 10:00:00"),
      ("s1", "u1", "web", "chrome", "click", "2026-02-09 10:05:00"),
      ("s1", "u1", "web", "chrome", "purchase", "2026-02-09 10:10:00"),
      ("s2", "u2", "ios", "safari", "view", "2026-02-09 11:00:00")
    ).toDF("sessionId", "userId", "platform", "browser", "eventType", "timestamp")
  }

  test("countStatistic should correctly pivot event types and fill nulls with 0") {
    val aggregateSession = new AggregateSession("2026-02-09")
    val inputDf = createInputData()

    val result = aggregateSession.countStatistic(inputDf)

    val row1 = result.filter(col("sessionId") === "s1").collect().head
    assertEquals(row1.getAs[Long]("totalViews"), 1L)
    assertEquals(row1.getAs[Long]("totalClicks"), 1L)
    assertEquals(row1.getAs[Long]("totalPurchases"), 1L)

    val row2 = result.filter(col("sessionId") === "s2").collect().head
    assertEquals(row2.getAs[Long]("totalViews"), 1L)
    assertEquals(row2.getAs[Long]("totalClicks"), 0L)
    assertEquals(row2.getAs[Long]("totalPurchases"), 0L)
  }

  test("getSessionDuration should calculate correct delta in seconds") {
    val aggregateSession = new AggregateSession("2026-02-09")
    val inputDf = createInputData()

    val result = aggregateSession.getSessionDuration(inputDf)

    val s1Duration = result.filter(col("sessionId") === "s1")
      .select("sessionDuration").collect().head.getAs[Double](0)

    assertEquals(s1Duration, 600.0)
  }

  test("joinData should combine statistics and duration") {
    val aggregateSession = new AggregateSession("2026-02-09")
    val inputDf = createInputData()

    val stats = aggregateSession.countStatistic(inputDf)
    val duration = aggregateSession.getSessionDuration(inputDf)

    val joined = aggregateSession.joinData(stats, duration)

    assert(joined.columns.contains("totalClicks"))
    assert(joined.columns.contains("sessionDuration"))
    assertEquals(joined.count(), 2L)
  }

  test("prepareForSink should contain all required columns including localDate") {
    val aggregateSession = new AggregateSession("2026-02-09")
    val sparkSession = spark
    import sparkSession.implicits._

    val dummyDf = Seq(
      ("u1", "s1", 600.0, 1L, 1L, 1L, "chrome", "web")
    ).toDF("userId", "sessionId", "sessionDuration", "totalClicks", "totalViews", "totalPurchases", "browser", "platform")

    val prepared = aggregateSession.prepareForSink(dummyDf)

    assert(prepared.columns.contains("localDate"))
    assertEquals(prepared.schema("localDate").dataType, DateType)
  }
}
