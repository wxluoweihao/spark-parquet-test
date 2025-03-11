package org.example

import org.apache.spark.sql.SparkSession

object DataFrameReadWithCorrectKey {

  /**
   * The main function is the entry point of the program.
   * It demonstrates how to read encrypted Parquet files using the correct keys.
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
      "columnProtectionKey:AAECAwQFBgcICQoCCCCODw== ,  footerProtectionKey:AAECAAECAAECAAECAAECAC==")

    // Try to read an encrypted parquet
    spark.read.parquet(baseDir + "/output/test").show
  }
}
