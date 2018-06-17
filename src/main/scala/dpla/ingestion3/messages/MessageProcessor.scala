package dpla.ingestion3.messages

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{Dataset, Row}


object MessageProcessor {

  def getAllMessages(ds: Dataset[Row]): Dataset[Row] = {
    ds.select("messages.message", "messages.level", "messages.field", "messages.id", "messages.value")
      .where("size(messages) != 0")
  }
  def getErrors(ds: Dataset[Row]): Dataset[Row] = {
    ds.select("id" ,"level").where("level=='ERROR'").distinct()
  }

  def getWarnings(ds: Dataset[Row]): Dataset[Row] = {
    ds.select("id" ,"level").where("level=='WARN'").distinct()
  }

  def getDistinctIdCount(df: Dataset[Row]): Long = {
    df.select("id").groupBy("id").count().count()
  }
}


/**
  * Taken from https://stackoverflow.com/questions/7539831/scala-draw-table-to-console
  * Credit to Duncan McGregor
  *
  * TODO better padding for values so table is not as compact
  */
object Tabulator {
  def format(table: Seq[Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield for (cell <- row) yield if (cell == null) 0 else cell.toString.length
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator ::
      rows.head ::
      rowSeparator ::
      rows.tail.toList :::
      rowSeparator ::
      List()).mkString("\n")

  def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = {
    val cells = for ((item, size) <- row.zip(colSizes))
      yield if (size == 0) "" else StringUtils.rightPad(item.toString, size)
    cells.mkString("|", "|", "|")
  }

  def rowSeparator(colSizes: Seq[Int]): String = colSizes map { "-" * _ } mkString("+", "+", "+")
}
