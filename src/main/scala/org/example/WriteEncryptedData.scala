package org.example

import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteEncryptedData {

  /**
   * The WriteEncryptedData object is used for writing encrypted Parquet files using 2 keys.
   * This program demonstrates how to read plain text data and write it back in an encrypted form.
   *
   * @param args Command line arguments, not used in this program.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parquet Data Encryption POC")
      .master("local[*]")
      .getOrCreate()

    val baseDir = WriteEncryptedData.getClass.getClassLoader.getResource("data").getPath;

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

    // Read csv data in plain text
    val df = spark.read.option("header", "true").csv(baseDir + "/input")
    df.show()

    // save data with encrypted data
    df
      .coalesce(1)
      .write
      // protect age column
      .option("parquet.encryption.column.keys" , "columnProtectionKey:age")
      .option("parquet.encryption.footer.key" , "footerProtectionKey")
      .option("encryption.algorithm", "AES_GCM_CTR_V1")
      .mode(SaveMode.Overwrite)
      .parquet(baseDir + "/output/test")

  }
}
