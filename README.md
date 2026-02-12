# Spark Capstone Project

A distributed data processing pipeline built with **Apache Spark 3.5**, **Scala**, and **Cats Effect 3**. This project processes high-volume event data through three primary stages: Sessionization, Session Aggregation, and Multi-dimensional OLAP Cube generation.

---

## Project Architecture

The pipeline follows a Medallion-style architecture:
1.  **Job 1: Sessionization:** Groups raw events into user sessions based on a 30-minute inactivity threshold.
2.  **Job 2: Aggregate Sessions:** Calculates per-session statistics and duration.
3.  **Job 3: OLAP Cube Generation:** Performs multi-layered aggregations for business reporting.

---

## 🔧 Build & Test

### 1. Run Unit Tests
The project uses **MUnit** with **Cats Effect** integration. To run all tests:
```bash
./gradlew :spark-jobs:test
```
### 2. Build Fat Jar
```bash
./gradlew :spark-jobs:shadowJar -PrunJob=com.sepavliuk.capstone.spark.AggregateSession
```

### 3. Running Job Locally
```bash
./gradlew :spark-jobs:runJob -PrunJob=com.sepavliuk.capstone.spark.OlapCubeGenerationJob -Pdate=2026-02-09
```
