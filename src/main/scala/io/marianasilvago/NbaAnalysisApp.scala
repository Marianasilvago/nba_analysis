package io.marianasilvago

import io.marianasilvago.solution.question1._
import io.marianasilvago.solution.question2.{ArenaRankByAttendees, AverageStartTimePerHomeTeam, StadiumWithMostAttendees, TeamRankByAttendees}
import io.marianasilvago.solution.question3.{TeamRankByToughestSchedule, TeamWithToughestSchedule}
import io.marianasilvago.utils.{ArgumentParser, FileWriterUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, SparkSession}

import java.text.SimpleDateFormat
import java.util.Locale
import scala.collection.mutable

object NbaAnalysisApp {
  val spark: SparkSession = SparkSession.builder()
    .appName("nba_analysis")
    .master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val appOption = ArgumentParser.getOptions(args)

    val schema = new StructType(Array(
      StructField("date", StringType, nullable = true),
      StructField("start_time", StringType, nullable = true),
      StructField("visitor_team", StringType, nullable = true),
      StructField("visitor_points", IntegerType, nullable = true),
      StructField("home_team", StringType, nullable = true),
      StructField("home_points", IntegerType, nullable = true),
      StructField("optional_1", StringType, nullable = true),
      StructField("optional_2", StringType, nullable = true),
      StructField("attendees", IntegerType, nullable = true),
      StructField("arena", StringType, nullable = true),
      StructField("notes", StringType, nullable = true)
    ))
    val df = spark.read.format("csv").option("header", "true").schema(schema).load(appOption.dataPath)

    val udfToDate = udf((dateStr: String) => {
      val formatter = new SimpleDateFormat("E MMM dd yyyy", Locale.ENGLISH)
      new java.sql.Date(formatter.parse(dateStr).getTime)
    })

    val replaceSuffix = (c: Column) => when(c.endsWith("p"), regexp_replace(c, "p", " PM")).otherwise(c)

    val cleanedStartTimeDf = df.withColumn("new_start_time", replaceSuffix(col("start_time")))

    val newStartTimeDf = cleanedStartTimeDf
      .withColumn("start_timestamp", to_timestamp(col("new_start_time"), "h:mm a"))

    val startTimestampDf = newStartTimeDf.drop("start_time", "new_start_time")
      .withColumnRenamed("start_timestamp", "start_time")

    val newDateDf = startTimestampDf
      .withColumn("new_date", udfToDate(col("date")))
      .drop("date")
      .withColumnRenamed("new_date", "date")

    val requiredDf = newDateDf.drop("optional_1", "optional_2", "notes")

    val cummSol1 = new mutable.StringBuilder
    cummSol1.append("Solutions for question 1: \n")

    val sol1 = HighestTeamScore.saveAndGetHighestTeamScore(requiredDf, appOption.outputPath, true)
    cummSol1.append(s"Highest_Team_Score = $sol1\n\n")

    val sol2 = HighestHomeTeamScore.saveAndGetHighestHomeTeamScore(requiredDf, appOption.outputPath, true)
    cummSol1.append(s"Highest_Home_Team_Score = $sol2\n\n")

    val sol3 = HighestVisitorTeamScore.saveAndGetHighestVisitorTeamScore(requiredDf, appOption.outputPath, true)
    cummSol1.append(s"Highest_Visitor_Team_Score = $sol3\n\n")

    val sol4 = WinRateForHomeTeam.saveAndGetWinRateForHomeTeam(requiredDf, appOption.outputPath, true)
    cummSol1.append(s"Win_Rate_For_Home_Team = $sol4\n\n")

    val sol5 = WinRateForVisitorTeam.saveAndGetWinRateForVisitorTeam(requiredDf, appOption.outputPath, true)
    cummSol1.append(s"Win_Rate_For_Visitor_Team = $sol5\n\n")

    val sol6 = TeamRank.saveAndGetTeamRanks(requiredDf, appOption.outputPath, true)
    cummSol1.append(s"Team_Rank = \n${sol6.mkString("\n")}\n\n")

    cummSol1.append("\n\n")

    val cummSol2 = new mutable.StringBuilder
    cummSol2.append("Solutions for question 2: \n")

    val sol7 = AverageStartTimePerHomeTeam.saveAndGetAverageStartTimePerHomeTeam(requiredDf, appOption.outputPath, true)
    cummSol2.append(s"Average_Start_Time_Per_Home_Team = \n${sol7.mkString("\n")}\n\n")

    val sol8 = StadiumWithMostAttendees.saveAndGetStadiumWithMostAttendees(requiredDf, appOption.outputPath, true)
    cummSol2.append(s"Stadium_With_Most_Attendees = $sol8\n\n")

    val sol9 = TeamRankByAttendees.saveAndGetTeamRanksByAttendees(requiredDf, appOption.outputPath, true)
    cummSol2.append(s"Team_Rank_By_Attendees = \n${sol9.mkString("\n")}\n\n")

    val sol10 = ArenaRankByAttendees.saveAndGetArenaRankByAttendees(requiredDf, appOption.outputPath, true)
    cummSol2.append(s"Arena_Rank_By_Attendees = \n${sol10.mkString("\n")}\n\n")

    cummSol2.append("\n\n")

    val cummSol3 = new mutable.StringBuilder
    cummSol3.append("Solutions for question 3: \n")

    val sol11 = TeamWithToughestSchedule.saveAndGetTeamWithToughestSchedule(requiredDf, appOption.outputPath, true)
    cummSol3.append(s"Team_With_Toughest_Schedule = $sol11\n\n")

    val sol12 = TeamRankByToughestSchedule.saveAndGetTeamRankWithToughestSchedule(requiredDf, appOption.outputPath, true)
    cummSol3.append(s"Team_Rank_By_Toughest_Schedule = \n${sol12.mkString("\n")}\n\n")

    cummSol3.append("\n\n")

    val outputLogs = Seq(cummSol1.toString(), cummSol2.toString(), cummSol3.toString())
    FileWriterUtil.writeFile(s"${appOption.outputPath}/Solution.log", outputLogs)
  }
}