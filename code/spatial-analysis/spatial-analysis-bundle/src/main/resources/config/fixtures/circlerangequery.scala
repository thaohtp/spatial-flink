package config.fixtures

import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.data.{DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.ExperimentSuite
import org.peelframework.dstat.beans.system.Dstat
import org.peelframework.flink.beans.experiment.FlinkExperiment
import org.peelframework.flink.beans.system.Flink
import org.peelframework.spark.beans.experiment.SparkExperiment
import org.peelframework.spark.beans.system.Spark
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** `Spatial analysis` experiment fixtures for the 'spatial-analysis' bundle. */
@Configuration
class circlerangequery extends ApplicationContextAware {
  val size = 1
  val runs = 3
  val nbDimension = 2
  val jarVersion = "1.0"
  val className = "benchmark.CircleRangeQueryBenchmark"

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
  val sampleRate = 0.1;
  val input1_s1 = "/home/hadoop/thaohtp/data/input/gdelt6gb/";
  val queryInput_s1 = "/home/hadoop/thaohtp/data/input/gdelt6gb/";
  val output_s1 = "/home/hadoop/thaohtp/data/output/dummy-test";
  val radius_s1 = 0.05;


  val input1_s5 = "/home/hadoop/thaohtp/data/input/gdelt6gb/";
  val queryInput_s5 = "/home/hadoop/thaohtp/data/input/gdelt6gb/";
  val output_s5 = "/home/hadoop/thaohtp/data/output/dummy-test";
  val radius_s5 = 0.05;


  val input1_s10 = "/home/hadoop/thaohtp/data/input/gdelt6gb/";
  val queryInput_s10 = "/home/hadoop/thaohtp/data/input/gdelt6gb/";
  val output_s10 = "/home/hadoop/thaohtp/data/output/dummy-test";
  val radius_s10 = 0.05;


  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }


  // ---------------------------------------------------
  // Experiments
  // ---------------------------------------------------

  @Bean(name = Array("crq.size1"))
  def `crq.size1`: ExperimentSuite = {
    val `crq.size1` = new FlinkExperiment(
      name = s"crq.size1.r" + sampleRate + ".node" + nbNodePerEntry + ".dis" + distance,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input1_s1
          +  "\" --queryinput \"" + queryInput_s1
          + "\" --output \"" + output_s1
          + "\" --samplerate " + sampleRate
          + " --nodeperentry " + nbNodePerEntry
          + " --nbdimension  " + nbDimension
          + " --radius " + radius_s1
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    new ExperimentSuite(Seq(
      `crq.size1`))
  }



  @Bean(name = Array("crq.size5"))
  def `crq.size5`: ExperimentSuite = {
    val `crq.size5` = new FlinkExperiment(
      name = s"crq.size5.r" + sampleRate + ".node" + nbNodePerEntry + ".dis" + distance,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input1_s5
          +  "\" --queryinput \"" + queryInput_s5
          + "\" --output \"" + output_s5
          + "\" --samplerate " + sampleRate
          + " --nodeperentry " + nbNodePerEntry
          + " --nbdimension  " + nbDimension
          + " --radius " + radius_s5
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    new ExperimentSuite(Seq(
      `crq.size5`))
  }

  @Bean(name = Array("crq.size10"))
  def `crq.size10`: ExperimentSuite = {
    val `crq.size10` = new FlinkExperiment(
      name = s"crq.size10.r" + sampleRate + ".node" + nbNodePerEntry + ".dis" + distance,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input1_s10
          +  "\" --queryinput \"" + queryInput_s10
          + "\" --output \"" + output_s10
          + "\" --samplerate " + sampleRate
          + " --nodeperentry " + nbNodePerEntry
          + " --nbdimension  " + nbDimension
          + " --radius " + radius_s10
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    new ExperimentSuite(Seq(
      `crq.size10`))
  }




}