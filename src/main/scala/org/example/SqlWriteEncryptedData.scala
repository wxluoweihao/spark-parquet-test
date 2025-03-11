package org.example

import org.apache.spark.sql.{SaveMode, SparkSession}

object SqlWriteEncryptedData {

  /**
   * Object for writing encrypted data using SQL.
   * This object demonstrates how to read plain text data, encrypt it using UDFs, and save the encrypted data to Parquet files.
   * @param args Command line arguments
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parquet Data Encryption POC")
      .master("local[*]")
      .getOrCreate()

    val baseDir = DataFrameReadWithCorrectKey.getClass.getClassLoader.getResource("data").getPath;

    // read csv data in plain text
    val df = spark.read.option("header", "true").csv(baseDir + "/input")
    df.createTempView("test_table")
    spark.sql("select * from test_table").show()

    // encrypt data using udf
    val encryptedDF = spark.sql(
      """
        |select
        |    id,
        |    name,
        |    base64(aes_encrypt(age, 'AAECAwQFBgcICQoCCCCODw==', 'CBC', 'DEFAULT')) as age,
        |    sex
        |from test_table
        |""".stripMargin);
    encryptedDF.show()
    encryptedDF.createTempView("test_table2")

    // save encrypted data to parquet
    encryptedDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(baseDir + "/output/test")

    // demonstrate data encrypt and decrypt using udf
    spark.sql(
      """
        |select
        |    id,
        |    name,
        |    age,
        |    cast(aes_decrypt(unbase64(age), 'AAECAwQFBgcICQoCCCCODw==', 'CBC', 'DEFAULT') as string) as decrypted_age,
        |    sex
        |from test_table2
        |""".stripMargin).show()

  }
}
