package io.marianasilvago.solution.question1

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class HighestHomeTeamScore(team: String, arena: String, totalScore: Long) {
  override def toString: String = s"Home team name: $team, Highest score: $totalScore, Home stadium: $arena"
}

object HighestHomeTeamScore {
  def saveAndGetHighestHomeTeamScore(df: DataFrame, outputPath: String, shouldSave: Boolean): HighestHomeTeamScore = {
    val homeDf = df.select(
      col("home_team").as("team"),
      col("home_points").as("points"),
      col("arena")
    )

    val highestHomeTeamScore = homeDf.groupBy("team", "arena")
      .agg(sum("points").alias("total_score"))
      .orderBy(desc("total_score"))
      .limit(1)

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(highestHomeTeamScore, s"${outputPath}/ques_1_highest_home_team_score")

    val result = highestHomeTeamScore.head()
    HighestHomeTeamScore(result.getString(0), result.getString(1), result.getLong(2))
  }
}
