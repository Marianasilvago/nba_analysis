package io.marianasilvago.solution.question1

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, sum}

case class HighestVisitorTeamScore(team: String, totalScore: Long) {
  override def toString: String = {
    s"Visitor team name: $team, Highest score: $totalScore"
  }
}

object HighestVisitorTeamScore {
  def saveAndGetHighestVisitorTeamScore(df: DataFrame, outputPath: String, shouldSave: Boolean): HighestVisitorTeamScore = {
    val visitorDf = df.select(
      col("visitor_team").as("team"),
      col("visitor_points").as("points")
    )

    val highestVisitorTeamScore = visitorDf.groupBy(col("team"))
      .agg(sum("points").alias("total_score"))
      .orderBy(desc("total_score"))
      .limit(1)

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(highestVisitorTeamScore, s"${outputPath}/ques_1_highest_visitor_team_score")

    val result = highestVisitorTeamScore.head()
    HighestVisitorTeamScore(result.getString(0), result.getLong(1))
  }
}
