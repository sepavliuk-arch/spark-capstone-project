package com.sepavliuk.capstone


import com.sepavliuk.capstone.spark.Sessionization
import munit.CatsEffectSuite
import org.apache.spark.sql.functions._

class SessionizationJobTest extends CatsEffectSuite with MUnitSharedSparkSession  {
  val job = new Sessionization

  test("splitOnSessions should increment sessionIndex when gap > 30 minutes") {
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      ("user1", "2023-10-01T10:00:00Z", "view"),
      ("user1", "2023-10-01T10:05:00Z", "view"),
      ("user1", "2023-10-01T10:40:00Z", "view")
    ).toDF("userId", "timestamp", "eventType")

    val result = job.splitOnSessions(data.withColumn("ts", to_timestamp(col("timestamp"))))
    val indexes = result.orderBy("ts").select("sessionIndex").collect().map(_.getLong(0)).toList

    assertEquals(indexes, List(1L, 1L, 2L))
  }

  test("splitOnSessions should force new session on 'login' event type") {
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      ("user1", "2023-10-01T10:00:00Z", "view"),
      ("user1", "2023-10-01T10:02:00Z", "login")
    ).toDF("userId", "timestamp", "eventType")

    val result = job.splitOnSessions(data.withColumn("ts", to_timestamp(col("timestamp"))))
    val indexes = result.orderBy("ts").select("sessionIndex").collect().map(_.getLong(0)).toList

    assertEquals(indexes, List(1L, 2L))
  }

  test("addSessionId should produce identical IDs for the same user session") {
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      ("user1", "2023-10-01T10:00:00Z", 1L),
      ("user1", "2023-10-01T10:05:00Z", 1L),
      ("user1", "2023-10-01T10:40:00Z", 2L)
    ).toDF("userId", "timestamp", "sessionIndex")
      .withColumn("timestamp", to_timestamp(col("timestamp")))

    val result = job.addSessionId(data)
    val ids = result.select("sessionId").collect().map(_.getInt(0))

    assertEquals(ids(0), ids(1), "Rows in same session should have same ID")
    assert(ids(0) != ids(2), "Rows in different sessions should have different IDs")
  }

  test("enrich should correctly parse browser and platform from userAgent") {
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      ("u1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (Chrome/120.0.0.0)"),
      ("u2", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (Safari/605.1.15)")
    ).toDF("userId", "userAgent")

    val result = job.enrich(data)
    val browsers = result.select("browser").collect().map(_.getString(0)).toList
    val platforms = result.select("platform").collect().map(_.getString(0)).toList

    assertEquals(browsers, List("Chrome", "Safari"))
    assertEquals(platforms, List("Windows NT 10.0", "Macintosh"))
  }

  test("sessionization should handle multiple users in parallel without interference") {
    val sparkSession = spark
    import sparkSession.implicits._
    val data = Seq(
      ("A", "2023-10-01T10:00:00Z"),
      ("B", "2023-10-01T10:00:00Z"),
      ("A", "2023-10-01T10:05:00Z"),
      ("B", "2023-10-01T10:05:00Z")
    ).toDF("userId", "timestamp")
      .withColumn("eventType", lit("view"))

    val result = job.splitOnSessions(data.withColumn("ts", to_timestamp(col("timestamp"))))
    val aCount = result.filter(col("userId") === "A").select("sessionIndex").distinct().count()
    val bCount = result.filter(col("userId") === "B").select("sessionIndex").distinct().count()

    assertEquals(aCount, 1L)
    assertEquals(bCount, 1L)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}
