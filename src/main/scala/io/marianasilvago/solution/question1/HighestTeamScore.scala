package io.marianasilvago.solution.question1

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, sum}

case class HighestTeamScore(team: String, totalScore: Long) {
  override def toString: String = s"Team name: $team, Highest score: $totalScore"
}

object HighestTeamScore {
  def saveAndGetHighestTeamScore(df: DataFrame, outputPath: String, shouldSave: Boolean): HighestTeamScore = {
    val visitorDf = df.select(
      col("visitor_team").as("team"),
      col("visitor_points").as("points")
    )

    val homeDf = df.select(
      col("home_team").as("team"),
      col("home_points").as("points")
    )

    val teamPointsDf = visitorDf.unionByName(homeDf)

    val highestTeamScore = teamPointsDf.groupBy(col("team"))
      .agg(sum("points").alias("total_score"))
      .orderBy(desc("total_score"))
      .limit(1)

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(highestTeamScore, s"${outputPath}/ques_1_highest_team_score")

    val result = highestTeamScore.head()
    HighestTeamScore(result.getString(0), result.getLong(1))
  }
}

