package io.marianasilvago.solution.question2

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, date_format, round}
import org.apache.spark.sql.types.LongType

case class AverageStartTimePerHomeTeam(team: String, arena: String, avgStartTime: String) {
  override def toString: String = s"Home team name: $team, Average start time: $avgStartTime, Home stadium: $arena"
}

object AverageStartTimePerHomeTeam {
  def saveAndGetAverageStartTimePerHomeTeam(df: DataFrame, outputPath: String, shouldSave: Boolean): List[AverageStartTimePerHomeTeam] = {
    val homeDf = df.select(
      col("home_team").as("team"),
      col("arena"),
      col("start_time")
    )

    val avgStartTimeLongPerTeamDf = homeDf.groupBy("team", "arena")
      .agg(round(avg(col("start_time").cast(LongType)), 4).alias("avg_start_time_long"))

    val avgStartTimeStampPerTeamDf = avgStartTimeLongPerTeamDf
      .withColumn("avg_start_timestamp", col("avg_start_time_long").cast("timestamp"))

    val avgStartTimePerTeamDf = avgStartTimeStampPerTeamDf
      .withColumn("avg_start_time", date_format(col("avg_start_timestamp"), "h:mm a"))

    val avgStartTimePerTeam = avgStartTimePerTeamDf.select(
      "team",
      "arena",
      "avg_start_time"
    )

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(avgStartTimePerTeam, s"${outputPath}/ques_2_avg_start_time_per_home_team")

    val results = avgStartTimePerTeam.collect()
    results.map(result => AverageStartTimePerHomeTeam(result.getString(0), result.getString(1), result.getString(2))).toList
  }
}
