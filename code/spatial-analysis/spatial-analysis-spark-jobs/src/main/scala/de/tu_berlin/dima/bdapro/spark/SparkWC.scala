package de.tu_berlin.dima.bdapro.spark

import org.apache.spark.{SparkConf, SparkContext}

/** A `WordCount` workload job for Spark. */
object SparkWC {

  def main(args: Array[String]) {
    if (args.length != 2) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = new SparkContext(new SparkConf().setAppName("spatial-analysis-spark"))
    spark.textFile(inputPath)
      .flatMap(_.toLowerCase.split("\\W+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(outputPath)
  }

}
