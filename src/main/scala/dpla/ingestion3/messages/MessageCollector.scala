package dpla.ingestion3.messages

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class MessageCollector[IngestMessage] {
  protected val messages: ListBuffer[IngestMessage] =
    ListBuffer[IngestMessage]()

  def add(msg: IngestMessage): messages.type = messages += msg

  def getAll: mutable.Seq[IngestMessage] = messages

  def deleteAll(): Unit = messages.remove(0, messages.size)
}
