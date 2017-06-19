import scala.collection.mutable.ArrayBuffer

import java.io.FileInputStream
import java.util.zip.{ZipEntry, ZipInputStream}

package object config {
//  w

  def getUsers(pkg: String, className: String): Seq[String] = {
    val f = s"apps/spatial-analysis-flink-jobs-1.0-SNAPSHOT.jar"
    val zip = new ZipInputStream(new FileInputStream(f))
    var entry: ZipEntry = zip.getNextEntry()
    val users = new ArrayBuffer[String]()
    while (entry != null) {
      if (!entry.isDirectory && entry.getName.endsWith(".class")) {
        val pattern = ("""de/tu_berlin/dima/""" + pkg + """/([^/]*)/""" + className + """\.class""").r
        val matches = pattern.findAllIn(entry.getName).matchData foreach {
          m => users += m.group(1)
        }
      }
      entry = zip.getNextEntry()
    }
    users
  }
}
