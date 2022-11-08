package io.marianasilvago.solution.question3

import io.marianasilvago.utils.DataFrameWriterUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class TeamRankByToughestSchedule(team: String, rank: Long) {
  override def toString: String = s"Team: $team , Rank: $rank"
}

object TeamRankByToughestSchedule {
  private def getTeamRankDf(df: DataFrame): DataFrame = {
    val window = Window
      .partitionBy("team")
      .orderBy("date")

    val previousDateDf = df.withColumn("previous_date", lag("date", 1).over(window))
      .na.drop(Seq("previous_date"))

    val consecutivePlaysDf = previousDateDf
      .withColumn("consecutive_plays", datediff(col("date"), col("previous_date")))
      .filter("consecutive_plays == 1")

    consecutivePlaysDf.groupBy("team")
      .agg(sum(col("consecutive_plays")).alias("rank"))
  }

  def saveAndGetTeamRankWithToughestSchedule(df: DataFrame, outputPath: String, shouldSave: Boolean): List[TeamRankByToughestSchedule] = {
    val positiveRankUdf = udf((c: Int) => c + 1)

    val visitorDf = df.select(
      col("date"),
      col("visitor_team").as("team")
    )

    val visitorTeamRank = getTeamRankDf(visitorDf)
      .withColumn("positive_rank", positiveRankUdf(col("rank")))
      .drop("rank")
      .withColumnRenamed("positive_rank", "rank")

    val negativeRankUdf = udf((c: Int) => c * (-1))

    val homeDf = df.select(
      col("date"),
      col("home_team").as("team")
    )

    val homeTeamRankDf = getTeamRankDf(homeDf)
      .withColumn("negative_rank", negativeRankUdf(col("rank")))
      .drop("rank")
      .withColumnRenamed("negative_rank", "rank")

    val teamRanks = visitorTeamRank.unionByName(homeTeamRankDf)
      .groupBy("team")
      .agg(sum("rank").alias("rank"))
      .orderBy(desc("rank"))

    if (shouldSave)
      DataFrameWriterUtil.writeAsCsv(teamRanks, s"${outputPath}/ques_3_team_rank_by_toughest_schedule")

    val results = teamRanks.collect()
    results.map(result => TeamRankByToughestSchedule(result.getString(0), result.getLong(1))).toList
  }
}
