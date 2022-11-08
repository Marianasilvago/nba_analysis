package io.marianasilvago.model
import picocli.CommandLine

class AppOption {
  @CommandLine.Option(names = Array("-f"), description = Array("file/folder containing data"), required = true)
  var dataPath: String = _

  @CommandLine.Option(names = Array("-o"), description = Array("output folder"), required = true)
  var outputPath: String = _
}