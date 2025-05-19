package agh.wggios.analizadanych.datawriter
import org.apache.spark.sql.DataFrame

class DataWriter {
    def write_csv(df: DataFrame, path: String): Unit = {
        df.write.mode("overwrite").csv(path)
    }
}
