package io.marianasilvago.solution.question1

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

case class WinRateForHomeTeam(team: String, arena: String, winRate: Double) {
  override def toString: String = s"Home team name: $team, Win rate: $winRate, Home stadium: $arena"
}

object WinRateForHomeTeam {
  def saveAndGetWinRateForHomeTeam(df: DataFrame, outputPath: String, shouldSave: Boolean): WinRateForHomeTeam = {
    val homeDf = df.select(
      col("home_team"),
      col("home_points"),
      col("visitor_points"),
      col("arena")
    )

    val countCondition = (condition: Column) => sum(when(condition, 1).otherwise(0))

    val homeTeamWinsDf = homeDf.groupBy("home_team", "arena")
      .agg(
        countCondition(col("home_points") > col("visitor_points")).alias("wins"),
        count(col("home_team")).alias("total_played")
      )

    val winRateDf = homeTeamWinsDf.withColumn("win_rate", round(col("wins") / col("total_played"), 4))
      .orderBy(desc("win_rate"))
      .limit(1)

    val winRateForHomeTeam = winRateDf.select("home_team", "arena", "win_rate")

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(winRateForHomeTeam, s"${outputPath}/ques_1_win_rate_for_home_team")

    val result = winRateForHomeTeam.head()
    WinRateForHomeTeam(result.getString(0), result.getString(1), result.getDouble(2))
  }
}
