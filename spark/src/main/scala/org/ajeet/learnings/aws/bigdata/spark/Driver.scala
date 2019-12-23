package org.ajeet.learnings.aws.bigdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions. _
/**
 * Add hadoop binaries in path to avoid below IOException
 *    java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
 */
object Driver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark on AWS")
      .master("local")
      .getOrCreate()

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/311_NYC_service_requests.csv")
      .filter(col("Closed Date").isNotNull)
      .show()
  }

}
