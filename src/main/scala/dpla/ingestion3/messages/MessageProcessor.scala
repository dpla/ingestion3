package dpla.ingestion3.messages

import org.apache.spark.sql.{Dataset, Row}

case class MessageFieldRpt(msg: String, field: String, count: Long)

object MessageProcessor {
  // Come back please...

  def getAllMessages(ds: Dataset[Row]): Dataset[Row] = {
    ds.select("messages.message", "messages.level", "messages.field", "messages.id", "messages.value")
      .where("size(messages) != 0")
  }
  def getErrors(ds: Dataset[Row]): Dataset[Row] = {
    ds.select("message", "level", "field", "id", "value").where("level=='ERROR'").distinct()
  }
  def getWarnings(ds: Dataset[Row]): Dataset[Row] = {
    ds.select("message", "level", "field", "id", "value").where("level=='WARN'").distinct()
  }
  def getDistinctIdCount(df: Dataset[Row]): Long = {
    df.select("id").groupBy("id").count().count()
  }
}