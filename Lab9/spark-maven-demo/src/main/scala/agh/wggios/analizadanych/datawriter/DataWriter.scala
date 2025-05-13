package agh.wggios.analizadanych.datawriter

class DataWriter {
  def save_file(df: DataFrame, outputPath: String, sep: String = ";", mode: String = "overwrite"): Unit = {
    df.write
      .option("header", "true")
      .option("delimiter", sep)
      .mode(mode)
      .csv(outputPath)
  }
}
