package dpla.ingestion3.harvesters.oai

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.nio.file.{Files, Path, Paths}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

/** Append-only progress log for OAI harvests.
  *
  * Writes one structured line per page request so the last line in the file
  * identifies exactly where a harvest failed.
  */
class OaiHarvestLogger(logDir: String, hubName: String) extends Serializable {

  private val formatter = DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    .withZone(ZoneId.systemDefault())

  @transient private lazy val writer: PrintWriter = {
    val dir = Paths.get(logDir)
    Files.createDirectories(dir)
    val timestamp = DateTimeFormatter
      .ofPattern("yyyyMMdd_HHmmss")
      .withZone(ZoneId.systemDefault())
      .format(Instant.now())
    val file = dir.resolve(s"oai-harvest-$hubName-$timestamp.log")
    new PrintWriter(new BufferedWriter(new FileWriter(file.toFile, true)))
  }

  def logPageRequest(info: OaiRequestInfo, url: String): Unit = {
    val now = formatter.format(Instant.now())
    val set = info.set.getOrElse("-")
    val token = info.resumptionToken.getOrElse("-")
    val cursor = info.cursor.map(_.toString).getOrElse("-")
    val total = info.completeListSize.map(_.toString).getOrElse("-")
    writer.println(
      s"$now | set=$set | token=$token | cursor=$cursor | total=$total | url=$url | status=pending"
    )
    writer.flush()
  }

  def logPageSuccess(info: OaiRequestInfo, url: String): Unit = {
    val now = formatter.format(Instant.now())
    val set = info.set.getOrElse("-")
    val token = info.resumptionToken.getOrElse("-")
    val cursor = info.cursor.map(_.toString).getOrElse("-")
    val total = info.completeListSize.map(_.toString).getOrElse("-")
    writer.println(
      s"$now | set=$set | token=$token | cursor=$cursor | total=$total | url=$url | status=ok"
    )
    writer.flush()
  }

  def logPageFailure(info: OaiRequestInfo, url: String, error: String): Unit = {
    val now = formatter.format(Instant.now())
    val set = info.set.getOrElse("-")
    val token = info.resumptionToken.getOrElse("-")
    val cursor = info.cursor.map(_.toString).getOrElse("-")
    val total = info.completeListSize.map(_.toString).getOrElse("-")
    val shortError = error.take(200).replace("\n", " ")
    writer.println(
      s"$now | set=$set | token=$token | cursor=$cursor | total=$total | url=$url | status=FAILED | error=$shortError"
    )
    writer.flush()
  }

  def close(): Unit = {
    writer.flush()
    writer.close()
  }
}

object OaiHarvestLogger {

  /** No-op logger that silently discards all log calls. */
  val Noop: OaiHarvestLogger = new OaiHarvestLogger("/dev/null", "noop") {
    override def logPageRequest(info: OaiRequestInfo, url: String): Unit = ()
    override def logPageSuccess(info: OaiRequestInfo, url: String): Unit = ()
    override def logPageFailure(
        info: OaiRequestInfo,
        url: String,
        error: String
    ): Unit = ()
    override def close(): Unit = ()
  }
}
