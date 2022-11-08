package io.marianasilvago.solution.question1

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class TeamRank(team: String, totalScore: Long, arena: String) {
  override def toString: String = s"Team: $team, Home arena: $arena, Total score: $totalScore"
}

object TeamRank {
  def saveAndGetTeamRanks(df: DataFrame, outputPath: String, shouldSave: Boolean): List[TeamRank] = {
    val homeDf = df.select(
      col("home_team").as("team"),
      col("home_points").as("points"),
      col("arena")
    )

    val teamRanksDf = homeDf.groupBy("team", "arena")
      .agg(sum("points").alias("total_score"))
      .orderBy(desc("total_score"))

    val teamRank = teamRanksDf.select(
      col("team").alias("Most scored home team"),
      col("total_score").alias("Scores at home stadium"),
      col("arena").alias("Home arena")
    )

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(teamRank, s"${outputPath}/ques_1_team_rank")

    val results = teamRank.collect()
    results.map(result => TeamRank(result.getString(0), result.getLong(1), result.getString(2))).toList
  }
}
