package dpla.ingestion3.harvesters.oai.refactor

import java.io.{File, FileWriter}

import com.univocity.parsers.csv._
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/**
  * OaiRelation for harvests that don't specify sets.
  *
  * @param oaiMethods        Implementation of the OaiMethods trait.
  * @param sqlContext        Spark sqlContext.
  */

class AllRecordsOaiRelation(oaiConfiguration: OaiConfiguration, @transient oaiMethods: OaiMethods)
                           (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  //list all the record pages, flat map to records
  override def buildScan(): RDD[Row] = {

    val tempFile = File.createTempFile("oai", ".txt")
    tempFile.deleteOnExit()
    cacheTempFile(tempFile)
    tempFileToRdd(tempFile)
  }

  private def tempFileToRdd(tempFile: File): RDD[Row] = {
    sqlContext.read.csv(tempFile.getAbsolutePath)
      .rdd
      .map(handleRow)
      //.flatMap({case Left(OaiPage(string)) => oaiMethods.parsePageIntoRecords(string)})
  }

  private def handleRow(row: Row): Either[OaiPage, OaiError] =
    row.getString(0) match {
      case "page" => Left(OaiPage(row.getString(1)))
      case "error" => Right(OaiError(row.getString(1), Option(row.getString(2))))
    }

  private def cacheTempFile(tempFile: File): Unit = {
    val fileWriter = new FileWriter(tempFile)

    val writerSettings = new CsvWriterSettings
    val writer = new CsvWriter(fileWriter, writerSettings)

    try {
      for (page <- oaiMethods.listAllRecordPages)
        writer.writeRow(eitherToArray(page))

    } finally {
      IOUtils.closeQuietly(fileWriter)
    }
  }

  private def eitherToArray(either: Either[OaiPage, OaiError]) = either match {
    case Left(OaiPage(string)) => Seq("page", string, null)
    case Right(OaiError(message, url)) => Seq("error", message, url)
  }



}