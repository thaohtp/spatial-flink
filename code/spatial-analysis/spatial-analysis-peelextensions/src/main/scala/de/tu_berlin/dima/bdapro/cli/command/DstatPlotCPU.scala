package de.tu_berlin.dima.bdapro.cli.command

import java.io.PrintWriter
import java.lang.{System => Sys}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.Instant

import anorm.SqlParser._
import anorm.{~, _}
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.loadConfig
import org.peelframework.core.results.DB
import org.peelframework.core.util.console._
import org.peelframework.core.util.shell
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service
import resource._

import scala.language.postfixOps

/** Query the database for the CPU performance of a particular experiment. */
@Service("dstat:plot:cpu")
class DstatPlotCPU extends Command {

  override val help = "query the database for the CPU performance of a particular experiment."

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--connection")
      .`type`(classOf[String])
      .dest("app.db.connection")
      .metavar("ID")
      .help("database config name (default: h2)")
    // arguments
    parser.addArgument("suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("experiments suite cpu performance to plot")
    parser.addArgument("experiment")
      .`type`(classOf[String])
      .dest("app.experiment.name")
      .metavar("EXP")
      .help("name of the experiment")
    parser.addArgument("run")
      .`type`(classOf[Int])
      .dest("app.experiment.run")
      .metavar("RUN")
      .help("run of the experiment")

    // option defaults
    parser.setDefault("app.db.connection", "h2")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.db.connection", ns.getString("app.db.connection"))
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
    Sys.setProperty("app.experiment.name", ns.getString("app.experiment.name"))
    Sys.setProperty("app.experiment.run", ns.getString("app.experiment.run"))

  }

  override def run(context: ApplicationContext) = {
    logger.info(s"Plotting CPU usage for suite '${Sys.getProperty("app.suite.name")}' from '${Sys.getProperty("app.db.connection")}'")

    // load application configuration
    implicit val config = loadConfig()

    // create database connection
    val connName = Sys.getProperty("app.db.connection")
    implicit val conn = DB.getConnection(connName)

    val suite = config.getString("app.suite.name")
    val experiment = config.getString("app.experiment.name")
    val run = config.getInt("app.experiment.run")

    try {

      logger.info(s"Querying min and max timestamps for ($suite, $experiment, $run)")
      // Querying min and max timestamp associated with the plotted points for the selected experiment
      val (tMin, tMax) = SQL(
        """
          |SELECT    min(ee.v_timestamp) AS t_min,
          |          max(ee.v_timestamp) AS t_max
          |FROM      experiment          AS ex,
          |          experiment_run      AS er,
          |          experiment_event    AS ee
          |WHERE     ex.id               =  er.experiment_id
          |AND       er.id               =  ee.experiment_run_id
          |AND       er.run              =  {run}
          |AND       ex.name             =  {experiment}
          |AND       ex.suite            =  {suite}
          |AND       ee.name             =  'dstat_total_cpu_usage:usr'
        """.stripMargin.trim
      ).on(
        "run" -> run,
        "experiment" -> experiment,
        "suite" -> suite
      ).as(
        {
          get[Instant]("t_min") ~ get[Instant]("t_max") map {
            case t_min ~ t_max => (t_min.toEpochMilli, t_max.toEpochMilli)
          }
        } *
      ).head

      logger.info(s"Querying Data CPU measurements for ($suite, $experiment, $run)")
      // Querying all relevant Dstat measurement data in format: (host, time, sys, usr)
      val data = {
        val stmt = conn.prepareStatement(
          """
            |SELECT   d1.host                   AS host,
            |         d1.v_timestamp            AS time,
            |         d1.v_double               AS usr,
            |         d2.v_double               AS sys
            |FROM     experiment                AS ex,
            |         experiment_run            AS er,
            |         experiment_event          AS d1,
            |         experiment_event          AS d2
            |WHERE    ex.id                     = er.experiment_id
            |AND      er.id                     = d1.experiment_run_id
            |AND      er.id                     = d2.experiment_run_id
            |AND      d1.v_timestamp            = d2.v_timestamp
            |AND      d1.host                   = d2.host
            |AND      er.run                    = ?
            |AND      ex.name                   = ?
            |AND      ex.suite                  = ?
            |AND      d1.name                   = 'dstat_total_cpu_usage:usr'
            |AND      d2.name                   = 'dstat_total_cpu_usage:sys'
            |ORDER BY host, time;
          """.stripMargin
        )

        stmt.setInt(1, run)
        stmt.setString(2, experiment)
        stmt.setString(3, suite)

        val rslt = stmt.executeQuery()

        val bldr = List.newBuilder[(String, Long, Double, Double)]

        while (rslt.next()) bldr += ((
          rslt.getString("host"),
          rslt.getTimestamp("time").toInstant.toEpochMilli,
          rslt.getDouble("sys"),
          rslt.getDouble("usr")))

        bldr.result()
      }

      // Querying total_cpu_usage data
      val gplsPath = Paths.get(config.getString("app.path.utils"), "gpl")
      val basePath = Paths.get(config.getString("app.path.results"), suite, "plots")
      val dataPath = basePath.resolve(s"$suite-$experiment-$run-cpu_performance.dat")

      // ensure base folder exists
      shell.ensureFolderIsWritable(basePath)

      // write data file
      logger.info(s"Writing Total CPU performance results into path '$dataPath'")

      for {
        bwriter <- managed(Files.newBufferedWriter(dataPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE))
        pwriter <- managed(new PrintWriter(bwriter))
      } {

        // Saving data to disk
        for (((host, time, sys, usr), i) <- data.zipWithIndex) {
          //@formatter:off
          pwriter.println(Seq(
            s"$host"                            padTo(30, ' '),
            f"${(time - tMin)/1000.0}%1.3f"     padTo(10, ' '),
            f"${usr/100.0}%1.4f"                padTo(10, ' '),
            f"${sys/100.0}%1.4f"                padTo(10, ' ')
          ) mkString "")
          //@formatter:on
        }
      }

      // unique hosts
      val hosts = data.map(_._1).distinct.mkString(" ")

      // execute gnuplot scripts
      logger.info(s"Plotting CPU performance results using Gnuplot")

      // plot CPU performance for all hosts for each run
      shell !(
        cmd =
          raw"""
             |gnuplot -e "gplsPath='$gplsPath'" \
             |        -e "basePath='$basePath'" \
             |        -e "dataPath='$dataPath'" \
             |        -e "suite='$suite'" \
             |        -e "experiment='$experiment'" \
             |        -e "run='$run'" \
             |        -e "hosts='$hosts'" \
             |        $gplsPath/DstatPlotCPU.gpl
             |        """.stripMargin.trim,
        errorMsg =
          "Cannot plot 'DstatPlotCPU.gpl' with Gnuplot")
    }
    catch {
      case e: Throwable =>
        logger.error(s"Error while plotting CPU performance results for suite '$suite'".red)
        throw e
    } finally {
      logger.info(s"Closing connection to database '$connName'")
      conn.close()
      logger.info("#" * 60)
    }
  }

}
