package de.tu_berlin.dima.bdapro.flink

import org.apache.flink.api.scala._

/** A `WordCount` workload job for Flink. */
object FlinkWC {

  def main(args: Array[String]) {
    if (args.length != 2) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.readTextFile(inputPath)
      .flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .writeAsCsv(outputPath)

    env.execute("spatial-analysis-flink")
  }

}
