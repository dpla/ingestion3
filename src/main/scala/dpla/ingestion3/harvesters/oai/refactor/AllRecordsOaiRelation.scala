package dpla.ingestion3.harvesters.oai.refactor

import java.io.{File, FileWriter}

import com.opencsv.CSVWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/**
  * OaiRelation for harvests that don't specify sets.
  *
  * @param oaiMethods Implementation of the OaiMethods trait.
  * @param sqlContext Spark sqlContext.
  */

class AllRecordsOaiRelation(oaiConfiguration: OaiConfiguration, oaiMethods: OaiMethods)
                           (@transient override val sqlContext: SQLContext)
  extends OaiRelation {

  override def buildScan(): RDD[Row] = {
    val tempFile = new File("oai.txt") // File.createTempFile("oai", ".txt")
    // tempFile.deleteOnExit()
    cacheTempFile(tempFile)
    tempFileToRdd(tempFile)
  }

  private[refactor] def tempFileToRdd(tempFile: File): RDD[Row] = {
    val csvRdd = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("mode", "FAILFAST")
      .load(tempFile.getAbsolutePath).rdd

    val eitherRdd = csvRdd.map(handleCsvRow)
    val pagesEitherRdd = eitherRdd.flatMap(oaiMethods.parsePageIntoRecords)
    pagesEitherRdd.map(OaiRelation.convertToOutputRow)
  }

  private[refactor] def handleCsvRow(row: Row): Either[OaiError, OaiPage] =
    row.toSeq match {
      case Seq("page", page: String, _) =>
        Right(OaiPage(page))
      // Changed from null to "" b/c empty strings are are written out
      // to the csv in lieu of null. See padTo() in cacheTempFile().
      case Seq("error", message: String, "") =>
        Left(OaiError(message, None))
      case Seq("error", message: String, url: String) =>
        Left(OaiError(message, Some(url)))
    }

  private[refactor] def cacheTempFile(tempFile: File): Unit = {
    //
    def t(n: Object*) = n

    val writer = new CSVWriter(new FileWriter(tempFile), ',', CSVWriter.DEFAULT_QUOTE_CHARACTER, '\\')

    try {
      for (page <- oaiMethods.listAllRecordPages()) {
        val pageArr = t(eitherToArray(page): _*)
        val line = pageArr
          .filter(o => o.isInstanceOf[String])
          .map(l => l.toString)
          .toArray
          .padTo(3,"")

        writer.writeNext(line)
      }
    } finally {
       writer.close()
    }
  }

  private[refactor] def eitherToArray(either: Either[OaiError, OaiPage]): Seq[String] =
    either match {
      case Right(OaiPage(string)) =>
        import scala.xml._
        Seq("page", xml.Utility.trim(XML.loadString(string)).toString(), null)
      case Left(OaiError(message, None)) =>
        Seq("error", message, null)
      case Left(OaiError(message, Some(url))) =>
        Seq("error", message, url)
    }

}