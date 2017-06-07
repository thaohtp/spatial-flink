package flink.datagen.flink

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.NumberSequenceIterator
import flink.datagen.flink.Distributions.{Zipf, Binomial, DiscreteUniform, DiscreteDistribution}
import flink.datagen.util.RanHash

object WordGenerator {

  val SEED = 0xC00FFEE

  def main(args: Array[String]): Unit = {

    if (args.length != 5) {
      Console.err.println("Usage: <jar> numberOfTasks tuplesPerTask sizeOfDictionary distribution[params] outputPath")
      System.exit(-1)
    }

    val numberOfTasks         = args(0).toInt
    val tuplesPerTask         = args(1).toLong
    val sizeOfDictionary      = args(2).toInt
    implicit val distribution = parseDist(sizeOfDictionary, args(3))
    val outputPath            = args(4)

//    val numberOfTasks         = coresPerWorker * numberOfWorkers
    val numberOfWords         = numberOfTasks * tuplesPerTask

    // generate dictionary of random words
    implicit val dictionary = new Dictionary(SEED, sizeOfDictionary).words()

    val environment = ExecutionEnvironment.getExecutionEnvironment

    environment
      // create a sequence [1 .. N] to create N words
      .fromParallelCollection(new NumberSequenceIterator(1, numberOfWords))
      // set up workers
      .setParallelism(numberOfTasks)
      // map every n <- [1 .. N] to a random word sampled from a word list
      .map(i => word(i))
      // write result to file
      .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE)

    environment.execute(s"WordGenerator[$numberOfWords]")
  }

  def word(i: Long)(implicit dictionary: Array[String], distribution: DiscreteDistribution) = {
    dictionary(distribution.sample(new RanHash(SEED + i).next()))
  }

  object Patterns {
    val DiscreteUniform = """(Uniform)""".r
    val Binomial = """Binomial\[(1|1\.0|0\.\d+)\]""".r
    val Zipf = """Zipf\[(\d+(?:\.\d+)?)\]""".r
  }

  def parseDist(card: Int, s: String): DiscreteDistribution = s match {
    case Patterns.DiscreteUniform(_) => DiscreteUniform(card)
    case Patterns.Binomial(a) => Binomial(card, a.toDouble)
    case Patterns.Zipf(a) => Zipf(card, a.toDouble)
  }

}
