package org.example

import org.apache.spark.sql.{SaveMode, SparkSession}

object SqlReadEncryptedData {

  /**
   * Read encrypted data using Spark SQL and AES decryption
   * @param args Command line arguments, not used in this example.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parquet Data Encryption POC")
      .master("local[*]")
      .getOrCreate()
    val baseDir = DataFrameReadWithCorrectKey.getClass.getClassLoader.getResource("data").getPath;

    val sc = spark.sparkContext

    // read encrypted directly
    val df = spark.read.parquet(baseDir + "/output/test")
    df.createTempView("test_table")
    df.show()

    // decrypt data using udf
    val encryptedDF = spark.sql(
      """
        |select
        |    id,
        |    name,
        |    cast(aes_decrypt(unbase64(age), 'AAECAwQFBgcICQoCCCCODw==', 'CBC', 'DEFAULT') as string) as age,
        |    sex
        |from test_table
        |""".stripMargin);
    encryptedDF.show()
  }
}
