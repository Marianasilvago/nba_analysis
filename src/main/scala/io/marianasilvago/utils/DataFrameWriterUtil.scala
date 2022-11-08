package io.marianasilvago.utils

import org.apache.spark.sql.DataFrame

object DataFrameWriterUtil {
  /**
   * Save dataframe at given output path.
   * @param df dataframe to save.
   * @param outputPath path to save the dataframe.
   */
  def writeAsCsv(df: DataFrame, outputPath: String): Unit = {
    df.write.option("header", value = true).csv(outputPath)
  }
}
