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
class distancejoin extends ApplicationContextAware {
  val size = 1
  val runs = 3
  val nbDimension = 2
  val jarVersion = "1.0"
  val className = "benchmark.DistanceJoinBenchmark"

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

  val server = "/home/hadoop/thaohtp/data";

  val input1_s1 = server + "/input/jointest/i1s1";
  val input1_s2 = server + "/input/jointest/i1s2";
  val input1_s3 = server + "/input/jointest/i1s3";
  val input1_s4 = server + "/input/jointest/i1s4";
  val input1_s5 = server + "/input/jointest/i1s5";

  val input2_s1 = server + "/input/jointest/i2s1";
  val input2_s2 = server + "/input/jointest/i2s2";
  val input2_s3 = server + "/input/jointest/i2s3";
  val input2_s4 = server + "/input/jointest/i2s4";
  val input2_s5 = server + "/input/jointest/i2s5";

  val output_s1 = server + "/output/distancejoin/size1";
  val output_s2 = server + "/output/distancejoin/size2";
  val output_s3 = server + "/output/distancejoin/size3";
  val output_s4 = server + "/output/distancejoin/size4";
  val output_s5 = server + "/output/distancejoin/size5";

  val parallel = 240
  val nbNodePerEntry = 64;
  val sampleRate = 0.1;
  val distance = 0.005;

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }


  // ---------------------------------------------------
  // Experiments
  // ---------------------------------------------------

  @Bean(name = Array("distancejoin"))
  def `dj.size1`: ExperimentSuite = {
    val `dj.size1` = new FlinkExperiment(
      name = s"dj.size1.r" + sampleRate + ".node" + nbNodePerEntry + ".dis" + distance,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input1 \"" + input1_s1
          +  "\" --input2 \"" + input2_s1
          + "\" --output \"" + output_s1
          + "\" --samplerate " + sampleRate
          + " --nodeperentry " + nbNodePerEntry
          + " --nbdimension  " + nbDimension
          + " --distance " + distance
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    val `dj.size2` = new FlinkExperiment(
      name = s"dj.size2.r" + sampleRate + ".node" + nbNodePerEntry + ".dis" + distance,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input1 \"" + input1_s2
          +  "\" --input2 \"" + input2_s2
          + "\" --output \"" + output_s2
          + "\" --samplerate " + sampleRate
          + " --nodeperentry " + nbNodePerEntry
          + " --nbdimension  " + nbDimension
          + " --distance " + distance
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    val `dj.size3` = new FlinkExperiment(
      name = s"dj.size3.r" + sampleRate + ".node" + nbNodePerEntry + ".dis" + distance,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input1 \"" + input1_s3
          +  "\" --input2 \"" + input2_s3
          + "\" --output \"" + output_s3
          + "\" --samplerate " + sampleRate
          + " --nodeperentry " + nbNodePerEntry
          + " --nbdimension  " + nbDimension
          + " --distance " + distance
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    val `dj.size4` = new FlinkExperiment(
      name = s"dj.size4.r" + sampleRate + ".node" + nbNodePerEntry + ".dis" + distance,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input1 \"" + input1_s4
          +  "\" --input2 \"" + input2_s4
          + "\" --output \"" + output_s4
          + "\" --samplerate " + sampleRate
          + " --nodeperentry " + nbNodePerEntry
          + " --nbdimension  " + nbDimension
          + " --distance " + distance
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    val `dj.size5` = new FlinkExperiment(
      name = s"dj.size5.r" + sampleRate + ".node" + nbNodePerEntry + ".dis" + distance,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input1 \"" + input1_s5
          +  "\" --input2 \"" + input2_s5
          + "\" --output \"" + output_s5
          + "\" --samplerate " + sampleRate
          + " --nodeperentry " + nbNodePerEntry
          + " --nbdimension  " + nbDimension
          + " --distance " + distance
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )


    new ExperimentSuite(Seq(
      `dj.size1`,
      `dj.size2`,
      `dj.size3`,
      `dj.size4`,
      `dj.size5`))
  }



}