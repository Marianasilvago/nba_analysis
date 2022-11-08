package io.marianasilvago.solution.question2

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, sum}

case class TeamRankByAttendees(team: String, totalAttendees: Long) {
  override def toString: String = {
    s"Team: $team, Total number of attendees: $totalAttendees"
  }
}

object TeamRankByAttendees {
  def saveAndGetTeamRanksByAttendees(df: DataFrame, outputPath: String, shouldSave: Boolean): List[TeamRankByAttendees] = {
    val visitorDf = df.select(col("visitor_team").as("team"), col("attendees"))

    val homeDf = df.select(
      col("home_team").as("team"),
      col("attendees")
    )

    val teamAttendeesDf = visitorDf.unionByName(homeDf)

    val mostAttendeesDf = teamAttendeesDf.groupBy("team")
      .agg(sum(col("attendees")).alias("total_attendees"))
      .orderBy(desc("total_attendees"))

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(mostAttendeesDf, s"${outputPath}/ques_2_team_rank_by_attendees")

    val results = mostAttendeesDf.collect()
    results.map(result => TeamRankByAttendees(result.getString(0), result.getLong(1))).toList
  }
}
