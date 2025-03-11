package org.example

import org.apache.spark.sql.SparkSession

object DataFrameReadWithWrongKeys {

  /**
   * Main entry point for reading Parquet files encrypted with incorrect keys.
   * This program demonstrates an attempt to read Parquet files using wrong encryption keys,
   * which should result in an error, proving the importance of correct key management in data encryption.
   *
   * @param args Command line arguments, not used in this example.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parquet Data Encryption POC")
      .master("local[*]")
      .getOrCreate()
    val baseDir = DataFrameReadWithCorrectKey.getClass.getClassLoader.getResource("data").getPath;


    val sc = spark.sparkContext

    // using testing kms
    sc.addJar("/home/mikeweihaoluo/projects/spark-parquet-test/target/scala-2.12/spark-parquet-test_2.12-0.1.jar")
    sc.hadoopConfiguration.set("parquet.crypto.factory.class" ,
      "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
    sc.hadoopConfiguration.set("parquet.encryption.kms.client.class" ,
      "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS")

    // Setting AES key for columns and footer
    sc.hadoopConfiguration.set("parquet.encryption.key.list" ,
      "columnProtectionKey:xxxxxxxxxxxx1 ,  footerProtectionKey:xxxxxxxxxxxxx2")

    // should throw error
    spark.read.parquet(baseDir + "/output/test").show
  }
}
