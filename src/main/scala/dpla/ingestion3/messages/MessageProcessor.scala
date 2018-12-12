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

    ds.select($"messages.message",
              $"messages.value",
              $"messages.field",
              $"messages.id",
              $"messages.level",
              $"messages.enrichedValue")
      .createOrReplaceTempView("messageTbl")

    spark.sql("""SELECT
                |level_exp AS level,
                |message_exp AS message,
                |field_exp AS field,
                |value_exp AS value,
                |enrichedValue_exp AS enrichedValue,
                |id_exp AS id
                |FROM messageTbl
                |LATERAL VIEW posexplode(message) message_exp AS message1, message_exp
                |LATERAL VIEW posexplode(value) value_exp AS value1, value_exp
                |LATERAL VIEW posexplode(field) field_exp AS field1, field_exp
                |LATERAL VIEW posexplode(id) id_exp AS id1, id_exp
                |LATERAL VIEW posexplode(level) level_exp AS level1, level_exp
                |LATERAL VIEW posexplode(enrichedValue) enrichedValue_exp AS enrichedValue1, enrichedValue_exp
                |WHERE message1 == value1 AND value1 == field1 AND field1 == id1 AND
                |id1 == level1 AND level1 == enrichedValue1
                |""".stripMargin)
  }

  def getErrors(ds: Dataset[Row]): Dataset[Row] =
    ds.select("message", "level", "field", "id", "value")
      .where(s"level=='${IngestLogLevel.error}'").distinct()

  def getWarnings(ds: Dataset[Row]): Dataset[Row] =
    ds.select("message", "level", "field", "id", "value")
      .where(s"level=='${IngestLogLevel.warn}'").distinct()

  def getDistinctIdCount(df: Dataset[Row]): Long =
    df.select("id").groupBy("id").count().count()

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
      s"- ${StringUtils.rightPad((k.msg.take(40) + ", " + k.field.take(20)).trim, 68, ".")}" +
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