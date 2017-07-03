package config.fixtures

import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.data.{DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.ExperimentSuite
import org.peelframework.dstat.beans.system.Dstat
import org.peelframework.flink.beans.experiment.FlinkExperiment
import org.peelframework.flink.beans.system.Flink
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** `Spatial analysis` experiment fixtures for the 'spatial-analysis' bundle. */
@Configuration
class spatialanalysiss10 extends ApplicationContextAware {

  val size = 10;
  val runs = 1;
  val nbDimension = 2;
  val jarVersion = "1.0";
  val className = "benchmark.IndexBenchmark";

  // ---------------------------------------------------
  // default-mac
  // ---------------------------------------------------
  val parallel = 4;
  val nbNodePerEntry = 40;
  val input = "/jml/data/test/spatial_analysis_flink/input/reduced_20170507.csv";
  val output = "/jml/data/test/spatial_analysis_flink/input/dummy_test";

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

  //  val parallel = 288
  //  val nbNodePerEntry = 40;
  //  val input = "/home/hadoop/thaohtp/data/input/gdelt6gb/";
  //  val output = "/home/hadoop/thaohtp/data/output/dummy-test";


  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }


  // ---------------------------------------------------
  // Experiments
  // ---------------------------------------------------

  @Bean(name = Array("sa.size10"))
  def `sa.size10`: ExperimentSuite = {
    val `sa.size10.r01` = new FlinkExperiment(
      name = s"sa.size10.r01.node" + nbNodePerEntry,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + " -p " + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input + "\" --output \"" + output + "\" --samplerate 0.1 --nodeperentry " + nbNodePerEntry + " --nbdimension  " + nbDimension
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    val `sa.size10.r02` = new FlinkExperiment(
      name = s"sa.size10.r02.node" + nbNodePerEntry,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + " -p " + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input + "\" --output \"" + output + "\" --samplerate 0.2 --nodeperentry " + nbNodePerEntry + " --nbdimension  " + nbDimension
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    val `sa.size10.r03` = new FlinkExperiment(
      name = s"sa.size10.r03.node" + nbNodePerEntry,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + " -p " + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input + "\" --output \"" + output + "\" --samplerate 0.3 --nodeperentry " + nbNodePerEntry + " --nbdimension  " + nbDimension
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )


    val `sa.size10.r04` = new FlinkExperiment(
      name = s"sa.size10.r04.node" + nbNodePerEntry,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + " -p " + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input + "\" --output \"" + output + "\" --samplerate 0.4 --nodeperentry " + nbNodePerEntry + " --nbdimension  " + nbDimension
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )


    val `sa.size10.r05` = new FlinkExperiment(
      name = s"sa.size10.r05.node" + nbNodePerEntry,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + " -p " + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input + "\" --output \"" + output + "\" --samplerate 0.5 --nodeperentry " + nbNodePerEntry + " --nbdimension  " + nbDimension
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    val `sa.size10.r06` = new FlinkExperiment(
      name = s"sa.size10.r06.node" + nbNodePerEntry,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + " -p " + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input + "\" --output \"" + output + "\" --samplerate 0.6 --nodeperentry " + nbNodePerEntry + " --nbdimension  " + nbDimension
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )


    val `sa.size10.r07` = new FlinkExperiment(
      name = s"sa.size10.r07.node" + nbNodePerEntry,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + " -p " + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input + "\" --output \"" + output + "\" --samplerate 0.7 --nodeperentry " + nbNodePerEntry + " --nbdimension  " + nbDimension
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    val `sa.size10.r08` = new FlinkExperiment(
      name = s"sa.size10.r08.node" + nbNodePerEntry,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + " -p " + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input + "\" --output \"" + output + "\" --samplerate 0.8 --nodeperentry " + nbNodePerEntry + " --nbdimension  " + nbDimension
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    val `sa.size10.r09` = new FlinkExperiment(
      name = s"sa.size10.r09.node" + nbNodePerEntry,
      command =
        (""" -v -c de.tu_berlin.dima.""" + className + " -p " + parallel +
          " ${app.path.apps}/spatial-analysis-flink-jobs-" + jarVersion + "-SNAPSHOT.jar " +
          "--input \"" + input + "\" --output \"" + output + "\" --samplerate 0.9 --nodeperentry " + nbNodePerEntry + " --nbdimension  " + nbDimension
          ).stripMargin.trim,
      config = ConfigFactory.parseString(""),
      runs = runs,
      systems = Set(ctx.getBean("dstat-0.7.3", classOf[Dstat])),
      runner = ctx.getBean("flink-1.2.0", classOf[Flink]),
      inputs = Set.empty[DataSet],
      outputs = Set.empty[ExperimentOutput]
    )

    new ExperimentSuite(Seq(
      `sa.size10.r01`,
      `sa.size10.r02`,
      `sa.size10.r03`,
      `sa.size10.r04`,
      `sa.size10.r05`,
      `sa.size10.r06`,
      `sa.size10.r07`,
      `sa.size10.r08`,
      `sa.size10.r09`))
  }

}