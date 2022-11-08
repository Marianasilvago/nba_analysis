package io.marianasilvago.solution.question2

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class StadiumWithMostAttendees(arena: String, totalAttendees: Long) {
  override def toString: String = {
    s"Home stadium: $arena, Total number of attendees: $totalAttendees"
  }
}

object StadiumWithMostAttendees {
  def saveAndGetStadiumWithMostAttendees(df: DataFrame, outputPath: String, shouldSave: Boolean): StadiumWithMostAttendees = {
    val arenaAttendeesDf = df.select(
      col("arena"),
      col("attendees")
    )

    val mostAttendeesDf = arenaAttendeesDf.groupBy("arena")
      .agg(sum(col("attendees")).alias("total_attendees"))
      .orderBy(desc("total_attendees"))
      .limit(1)

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(mostAttendeesDf, s"${outputPath}/ques_2_stadium_with_most_attendees")

    val result = mostAttendeesDf.head()
    StadiumWithMostAttendees(result.getString(0), result.getLong(1))
  }
}
