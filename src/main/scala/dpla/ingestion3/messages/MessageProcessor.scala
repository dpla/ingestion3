package dpla.ingestion3.messages


import dpla.ingestion3.utils.Utils
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.{Failure, Success, Try}

case class MessageFieldRpt(msg: String, field: String, count: Long)

object MessageProcessor{
  def getAllMessages(ds: Dataset[Row])
                    (implicit spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    ds.select("messages.message", "messages.level", "messages.field", "messages.id", "messages.value")
      .where("size(messages) != 0")
      .withColumn("level", explode($"level"))
      .withColumn("message", explode($"message"))
      .withColumn("field", explode($"field"))
      .withColumn("value", explode($"value"))
      .withColumn("id", explode($"id"))
      .distinct()
  }
  def getErrors(ds: Dataset[Row]): Dataset[Row] = {
    ds.select("message", "level", "field", "id", "value").where(s"level=='${IngestLogLevel.error}'").distinct()
  }
  def getWarnings(ds: Dataset[Row]): Dataset[Row] = {
    ds.select("message", "level", "field", "id", "value").where(s"level=='${IngestLogLevel.warn}'").distinct()
  }
  def getDistinctIdCount(df: Dataset[Row]): Long = {
    df.select("id").groupBy("id").count().count()
  }

  /**
    * Builds group by and count summary from `ds`. For example:
    *  Unable to mint URI       isShownAt   5
    *  Missing required field   isShownAt   1
    *
    * @param ds
    * @return String
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

    val topLine = msgFieldRptArray.map { case (k: MessageFieldRpt) =>
      s"- ${StringUtils.rightPad(k.msg.take(40) + " " + k.field.take(20), 68, ".")}" +
        s"${StringUtils.leftPad(Utils.formatNumber(k.count), 10, ".")}"
      case _ => ""
    }

    val dsCount = ds.count()

    val bottomLine = if (dsCount > 0)
      s"- Total${"."*(80-7-Utils.formatNumber(dsCount).length)}${Utils.formatNumber(dsCount)}"
    else ""

    (topLine ++ Array(bottomLine)).filter(_.nonEmpty)
  }
}