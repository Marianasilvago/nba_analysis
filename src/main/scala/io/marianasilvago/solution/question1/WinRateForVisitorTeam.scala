package io.marianasilvago.solution.question1

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

case class WinRateForVisitorTeam(team: String, winRate: Double) {
  override def toString: String = s"Visitor team name: $team, Win rate: $winRate"
}

object WinRateForVisitorTeam {
  def saveAndGetWinRateForVisitorTeam(df: DataFrame, outputPath: String, shouldSave: Boolean): WinRateForVisitorTeam = {
    val visitorDf = df.select(
      col("visitor_team"),
      col("home_points"),
      col("visitor_points")
    )

    val countCondition = (condition: Column) => sum(when(condition, 1).otherwise(0))

    val visitorTeamWinsDf = visitorDf.groupBy("visitor_team")
      .agg(
        countCondition(col("visitor_points") > col("home_points")).alias("wins"),
        count(col("visitor_team")).alias("total_played")
      )

    val winRateDf = visitorTeamWinsDf.withColumn("win_rate", round(col("wins") / col("total_played"), 4))
      .orderBy(desc("win_rate"))
      .limit(1)

    val winRateForVisitorTeam = winRateDf.select("visitor_team", "win_rate")

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(winRateForVisitorTeam, s"${outputPath}/ques_1_win_rate_for_visitor_team")

    val result = winRateForVisitorTeam.head()
    WinRateForVisitorTeam(result.getString(0), result.getDouble(1))
  }
}
