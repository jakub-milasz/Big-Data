package agh.wggios.analizadanych.datareader

import agh.wggios.analizadanych
import org.apache.spark.sql.DataFrame
import agh.wggios.analizadanych.{LoggingUtils, SparkSessionProvider}
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}

class DataReader extends SparkSessionProvider {

  def transformColumn(df: DataFrame, columnName: String, targetType: DataType): DataFrame = {
    df.withColumn(columnName, col(columnName).cast(targetType))
  }
}
