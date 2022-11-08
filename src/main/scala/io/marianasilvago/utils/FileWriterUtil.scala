package io.marianasilvago.utils

import java.io.{BufferedWriter, File, FileWriter}

object FileWriterUtil {
  /**
   * write a `Seq[String]` to the `filename`.
   *
   */

  /**
   * Write a `Seq[String]` to the `filePath`.
   *
   * @param filePath Path of file to write the data.
   * @param lines    Seq of strings.
   */
  def writeFile(filePath: String, lines: Seq[String]): Unit = {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
    }
    bw.close()
  }
}
