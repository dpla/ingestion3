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

class AllRecordsOaiRelation(oaiConfiguration: OaiConfiguration, @transient val oaiMethods: OaiMethods)
                           (@transient override val sqlContext: SQLContext)
  extends OaiRelation {

  override def buildScan(): RDD[Row] = {
    val tempFile = File.createTempFile("oai", ".txt")
    tempFile.deleteOnExit()
    cacheTempFile(tempFile)
    tempFileToRdd(tempFile)
  }

  private def tempFileToRdd(tempFile: File): RDD[Row] = {
    val csvRdd = sqlContext.read.csv(tempFile.getAbsolutePath).rdd
    val eitherRdd = csvRdd.map(handleCsvRow)
    val pagesEitherRdd = eitherRdd.flatMap(oaiMethods.parsePageIntoRecords)
    pagesEitherRdd.map(OaiRelation.convertToOutputRow)
  }

  private[refactor] def handleCsvRow(row: Row): Either[OaiError, OaiPage] =
    row.getString(0) match {
      case "page" => Right(OaiPage(row.getString(1)))
      case "error" => Left(OaiError(row.getString(1), Option(row.getString(2))))
    }

  private[refactor] def cacheTempFile(tempFile: File): Unit = {
    val fileWriter = new FileWriter(tempFile)
    val writerSettings = new CsvWriterSettings
    val writer = new CsvWriter(fileWriter, writerSettings)

    try {
      for (page <- oaiMethods.listAllRecordPages())
        writer.writeRow(eitherToArray(page))

    } finally {
      IOUtils.closeQuietly(fileWriter)
    }
  }

  private def eitherToArray(either: Either[OaiError, OaiPage]) = either match {
    case Right(OaiPage(string)) => Seq("page", string, null)
    case Left(OaiError(message, url)) => Seq("error", message, url)
  }

}