package config.fixtures

import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.data.{DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.ExperimentSuite
import org.peelframework.dstat.beans.system.Dstat
import org.peelframework.spark.beans.experiment.SparkExperiment
import org.peelframework.spark.beans.system.Spark
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** `Spatial analysis` experiment fixtures for the 'spatial-analysis' bundle. */
@Configuration
class simba1 extends ApplicationContextAware {

  val size = 1
  val runs = 3
  val nbDimension = 2
  val jarVersion = "1.0"
  val className = "benchmark.IndexBenchmark"

  // ---------------------------------------------------
  // default-mac
  // ---------------------------------------------------
//  val parallel = 4
//  val nbNodePerEntry = 40
//  val input = "/jml/data/test/spatial_analysis_flink/input/reduced_20170507.csv"
//  val output = "/jml/data/test/spatial_analysis_flink/input/dummy_test"

  // ---------------------------------------------------
  // ubuntu
  // ---------------------------------------------------

  //  val parallel = 1
  //  val nbNodePerEntry = 40;
  //  val input = "/jml/data/test/spatial_analysis_flink/input/reduced_20170507.csv";
  //  val output = "/home/parallels/mac/data/output/";


  // ---------------------------------------------------
  // ibm-power-1
  // ---------------------------------------------------

    val parallel = 240
    val nbNodePerEntry = 64;
    val input = "/home/hadoop/thaohtp/data/input/gdelt6gb/";
    val output = "/home/hadoop/thaohtp/data/output/dummy-test";


  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }


  // ---------------------------------------------------
  // Experiments
  // ---------------------------------------------------

  @Bean(name = Array("sb.size1"))
  def `sb.size1`: ExperimentSuite = {
    val `sb.size1` = new SparkExperiment(
      name = s"sb.size1",
      command =
        (""" --class org.apache.spark.sql.simba.examples.BenchMarkIndex """ +
          " ${app.path.apps}/simba_2.11-1.0.jar " +
          parallel + " " +
          "\"" + input + "\" "
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner  = ctx.getBean("spark-2.1.0", classOf[Spark]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )



    new ExperimentSuite(Seq(
      `sb.size1`))
  }


}