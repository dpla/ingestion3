package dpla.ingestion3.messages

import scala.collection.mutable.ListBuffer

class MessageCollector[IngestMessage] {
  protected val messages: ListBuffer[IngestMessage] =
    ListBuffer[IngestMessage]()

  def add(msg: IngestMessage) = messages += msg

  def getAll() = messages

  def deleteAll() = messages.remove(0, messages.size)
}
