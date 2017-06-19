package de.tu_berlin.dima.bdapro.cli.command

import java.lang.{System => Sys}
import java.nio.file.Paths

import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.peelframework.core.cli.command.Command
import org.peelframework.core.util.console.ConsoleColorise
import org.peelframework.core.util.shell
import org.scalactic.{Bad, Good}
import org.springframework.context.ApplicationContext

abstract class MergeTask extends Command {

  type Valid = Unit
  type Invalid = Error

  case class Error(msg: String)

  def pass = Good(())

  def fail(msg: String) = Bad(Error(msg))

  val Commit = """([0-9a-f]+) (.+)""".r

  val Additions = """(\d+) file changed, (\d+) insertions\(\+\)""".r

  val Path = """(\S+)\W+\|\W+(\d+)\W+\++""".r

  def commitFle(user: String): String = Paths.get(
    "spatial-analysis-flink-jobs",
    "src", "main",
    "scala", "de", "tu_berlin", "dima", "bdapro",
    "flink", taskBranch, toLowerCaseAndUnderscore(user), s"${taskName.replaceAll("\\W", "")}.scala"
  ).toString

  def commitMsg(user: String): String

  val taskName: String

  val taskBranch: String

  override val help = s"Merge solutions for the $taskName task."

  override def register(parser: Subparser) = {
    // arguments
    parser.addArgument("base-commit")
      .`type`(classOf[String])
      .dest("app.merge.commit.base")
      .metavar("HASH")
      .help("base commit")
    parser.addArgument("username")
      .`type`(classOf[String])
      .dest("app.merge.remote.user")
      .metavar("USER")
      .help("GitHub username")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.merge.local.src", Paths.get(Sys.getenv("BUNDLE_SRC"), "spatial-analysis").toAbsolutePath.toString)
    Sys.setProperty("app.merge.remote.user", ns.getString("app.merge.remote.user"))
    Sys.setProperty("app.merge.commit.base", ns.getString("app.merge.commit.base"))
  }

  override def run(context: ApplicationContext) = {
    val locSrc = Sys.getProperty("app.merge.local.src")
    val remUsr = Sys.getProperty("app.merge.remote.user")
    val basCmt = Sys.getProperty("app.merge.commit.base")

    logger.info(s"Attempting to merge implementation for '$taskName' task for user '$remUsr' ")
    logger.info(s"Bundle source is '$locSrc'")

    val status = for {
      _ <- {
        logger.info(s"Creating branch warmup-solutions (or rebasing it to master if it exists)")

        val ret1 = shell !
          s"""
             |cd $locSrc;
             |git checkout -q master
             |git checkout -q -b warmup-solutions;
             |git checkout -q warmup-solutions;
             |git rebase master
           """.stripMargin

        if (ret1 == 0) pass
        else fail(s"Problem when creating or rebasing branch warmup-solutions")

        logger.info(s"Preparing local branch '$remUsr-$taskBranch' based on $basCmt")

        val ret2 = shell !
          s"""
             |cd $locSrc;
             |git branch -D $remUsr-$taskBranch;
             |git checkout -b $remUsr-$taskBranch;
             |git reset --hard $basCmt;
              """.stripMargin

        if (ret2 == 0) pass
        else fail(s"Cannot prepare local branch '$remUsr-$taskBranch'")
      }
      _ <- {
        logger.info(s"Pulling '$remUsr/$taskBranch'")

        val ret = shell !
          s"""
             |cd $locSrc;
             |git pull --ff-only git@github.com:$remUsr/BDAPRO.WS1617.git $taskBranch;
              """.stripMargin

        if (ret == 0) pass
        else fail(s"Cannot pull '$remUsr/$taskBranch'")
      }
      _ <- {
        logger.info(s"Validating commits on '$remUsr-$taskBranch'")

        val ret = shell !!
          s"""
             |cd $locSrc;
             |git checkout -q warmup-solutions;
             |git log -n2 --oneline $remUsr-$taskBranch
          """.stripMargin

        ret.split('\n').map(_.trim).toList match {
          case Commit(solCmt, solMsg) :: Commit(basID, _) :: Nil =>
            def expMsg = commitMsg(remUsr)
            val expFle = commitFle(remUsr)

            if (!(basCmt startsWith basID)) {
              fail(s"Solution is not within a single commit based on $basCmt")
            } else if (!(solMsg.trim.toLowerCase == expMsg.trim.toLowerCase)) {
              fail(s"Malformed commit message for solution:\nexp:  $expMsg\ngot:  $solMsg")
            } else {
              val logStat = shell !!
                s"""
                   |cd $locSrc;
                   |git log -n1 --stat=300 $remUsr-$taskBranch
                """.stripMargin

              logStat.split('\n').map(_.trim).reverse.toList match {
                case Additions("1", _) :: Path(`expFle`, _) :: _ =>
                  pass
                case Additions("1", _) :: y :: rest =>
                  fail(s"Commit does not consist of a single file located at '$expFle'")
              }
            }

          case _ =>
            fail(s"Cannot validate commits on '$remUsr-$taskBranch'")
        }
      }
      _ <- {
        logger.info(s"Cherry-picking last commit of '$remUsr-$taskBranch'")

        val ret = shell !
          s"""
             |cd $locSrc;
             |git checkout -q warmup-solutions;
             |git cherry-pick $remUsr-$taskBranch
          """.stripMargin

        if (ret == 0) pass
        else fail(s"Cannot pull '$remUsr/$taskBranch'")
      }
    } yield pass

    status match {
      case Good(_) =>
        logger.info("Everything is fine".green)
      case Bad(Error(msg)) =>
        logger.error(s"Error while merging: $msg".red)
    }
  }

  def toLowerCaseAndUnderscore(s: String): String = {
    s.toLowerCase.replace('-','_')
  }
}
