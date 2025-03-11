package org.example

import org.apache.spark.sql.SparkSession

object DirectDataFrameRead {
  /**
   * The main function, used to demonstrate reading encrypted Parquet files without any keys.
   * @param args Command line arguments, not used in this example.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parquet Data Encryption POC")
      .master("local[*]")
      .getOrCreate()

    val baseDir = DataFrameReadWithCorrectKey.getClass.getClassLoader.getResource("data").getPath;
    spark.read.parquet(baseDir + "/output/test").show
  }
}
