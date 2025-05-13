package agh.wggios.analizadanych

import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging

trait SparkSessionProvider extends Logging {
  val spark = SparkSession.builder()
    .appName("Testowa")
    .config("spark.driver.memory","2000m")
    .master("local[4]")
    .getOrCreate()

  LoggingUtils.setupLogging()

  def getSparkSession(): SparkSession = {
    return  spark
  }

  def readFile(sep: String, path: String): DataFrame = {
    spark.read.options(Map("delimiter"->sep, "header"->"true")).csv(path)
  }

  def transformColumn(df: DataFrame, columnName: String, targetType: DataType): DataFrame = {
    df.withColumn(columnName, col(columnName).cast(targetType))
  }

  def saveData(df: DataFrame, outputPath: String, sep: String = ";", mode: String = "overwrite"): Unit = {
    df.write
      .option("header", "true")
      .option("delimiter", sep)
      .mode(mode)
      .csv(outputPath)
  }

  // Testowanie nowych metod
  val df2 = readFile(";", path)
  df2.show()
  df2.printSchema()
  val dfTyped = transformColumn(df2, "EnergiaWyprodukowana", DoubleType)
  dfTyped.printSchema()
  saveData(df2, "NewPhotovoltaics.csv")

}