package io.marianasilvago.solution.question2

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

case class ArenaRankByAttendees(arena: String, avgStartTime: String, totalAttendants: Long) {
  override def toString: String = s"Home Arena: $arena, Game average start time: $avgStartTime, Total Attendants: $totalAttendants"
}

object ArenaRankByAttendees {
  def saveAndGetArenaRankByAttendees(df: DataFrame, outputPath: String, shouldSave: Boolean): List[ArenaRankByAttendees] = {
    val homeDf = df.select(
      col("arena"),
      col("start_time"),
      col("attendees")
    )

    val avgStartTimeLongPerArenaDf = homeDf.groupBy("arena")
      .agg(
        round(avg(col("start_time").cast(LongType)), 4).alias("avg_start_time_long"),
        sum(col("attendees")).alias("total_attendees")
      )

    val avgStartTimeStampPerArenaDf = avgStartTimeLongPerArenaDf
      .withColumn("avg_start_timestamp", col("avg_start_time_long").cast("timestamp"))

    val avgStartTimePerArenaDf = avgStartTimeStampPerArenaDf
      .withColumn("avg_start_time", date_format(col("avg_start_timestamp"), "h:mm a"))

    val avgStartTimePerArena = avgStartTimePerArenaDf.select(
      col("arena").alias("Home Arena"),
      col("avg_start_time").alias("Game average start time"),
      col("total_attendees").alias("Total Attendants")
    ).orderBy(desc("Total Attendants"))

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(avgStartTimePerArena, s"${outputPath}/ques_2_arena_rank_by_attendees")

    val results = avgStartTimePerArena.collect()
    results.map(result => ArenaRankByAttendees(result.getString(0), result.getString(1), result.getLong(2))).toList
  }
}
