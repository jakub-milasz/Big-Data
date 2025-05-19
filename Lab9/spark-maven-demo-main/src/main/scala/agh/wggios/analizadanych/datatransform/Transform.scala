package agh.wggios.analizadanych.datatransform

import agh.wggios.analizadanych
import org.apache.spark.sql.DataFrame
import agh.wggios.analizadanych.{LoggingUtils, SparkSessionProvider}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col


import java.nio.file.{Files, Paths}
import org.apache.spark.sql.functions._

class DataTransform extends SparkSessionProvider {
    def transform_column(df: DataFrame, columnName: String, targetType: DataType): DataFrame = {
      logInfo("Zmiana typu kolumny")
      df.withColumn(columnName, col(columnName).cast(targetType))
    }
}
