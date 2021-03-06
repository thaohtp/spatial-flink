package config.fixtures

import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.data.{CopiedDataSet, DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.ExperimentSuite
import org.peelframework.dstat.beans.system.Dstat
import org.peelframework.flink.beans.experiment.FlinkExperiment
import org.peelframework.flink.beans.system.Flink
import org.peelframework.hadoop.beans.system.HDFS2
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** `Palindrome` experiment fixtures for the 'spatial-analysis' bundle. */
@Configuration
class palindrome extends ApplicationContextAware {

  val runs = 1

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // ---------------------------------------------------
  // Data Sets
  // ---------------------------------------------------

  @Bean(name = Array("palindrome.input"))
  def `palindrome.input`: DataSet = new CopiedDataSet(
    src = "${app.path.datasets}/palindromeSmall.txt",
    dst = "${system.hadoop-2.path.input}/palindromeSmall.txt",
    fs = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  // ---------------------------------------------------
  // Experiments
  // ---------------------------------------------------

  def palindromeSmall(name: String): FlinkExperiment = new FlinkExperiment(
    name = s"palindromeSmall.$name",
    command =
      s"""
         |-v -c de.tu_berlin.dima.bdapro.flink.palindrome.${name}.Palindrome \\
         |  $${app.path.apps}/spatial-analysis-flink-jobs-1.0-SNAPSHOT.jar \\
         |  $${system.hadoop-2.path.input}/palindromeSmall.txt
      """.stripMargin.trim,
    config = ConfigFactory.parseString(""),
    runs = runs,
    systems = Set(ctx.getBean("dstat-0.7.2", classOf[Dstat])),
    runner = ctx.getBean("flink-1.0.3", classOf[Flink]),
    inputs = Set(ctx.getBean("palindrome.input", classOf[DataSet])),
    outputs = Set.empty[ExperimentOutput]
  )

  @Bean(name = Array("palindrome"))
  def palindrome: ExperimentSuite = {
    new ExperimentSuite(config.getUsers("palindrome", "Palindrome").map(palindromeSmall))
  }
}