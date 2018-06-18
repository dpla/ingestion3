package dpla.ingestion3.messages

import dpla.ingestion3.utils.Utils
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.{Failure, Success, Try}

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

  /**
    *
    * @param ds
    * @return
    */
  def getMessageFieldSummary(ds: Dataset[Row])(implicit spark: SparkSession): Array[String] = {

    import spark.implicits._

    val d2 = ds.select("message", "field", "id")
      .groupBy("message", "field")
      .agg(count("id")).as("count")
      .orderBy("message","field")
      .orderBy(desc("count(id)"))


    val msgRptDs = d2.map( { case Row(msg: String, field: String, count: Long) => MessageFieldRpt(msg, field, count) })

    val singleColDs = msgRptDs.select(concat(col("msg"), lit("|"),col("field"), lit("|"),col("count"))).as("label")

    val rowAsString = singleColDs
      .collect()
      .map(row => row.getString(0))

    val splitLines = rowAsString.map(_.split("\\|"))

    val msgFieldRptArray = splitLines.map(arr =>
      MessageFieldRpt(
        msg = arr(0), field = arr(1),
        Try {arr(2).toLong} match { case Success(s) => s case Failure(_) => -1 }
      ))

    msgFieldRptArray.map { case (k: MessageFieldRpt) =>
      s"${StringUtils.rightPad(k.msg, 25, " ")} " +
        s"${StringUtils.rightPad(k.field, 15, " ")} " +
        s"${StringUtils.leftPad(Utils.formatNumber(k.count), 6, " ")}"
    }
  }
}