package dpla.ingestion3.harvesters.oai.refactor

import java.io.{File, FileWriter}

import com.opencsv.CSVWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/** OaiRelation for harvests that don't specify sets.
  *
  * @param oaiMethods
  *   Implementation of the OaiMethods trait.
  * @param sqlContext
  *   Spark sqlContext.
  */

class AllRecordsOaiRelation(
    oaiConfiguration: OaiConfiguration,
    oaiMethods: OaiMethods
)(@transient override val sqlContext: SQLContext)
    extends OaiRelation {

  override def buildScan(): RDD[Row] = {
    val tempFile = File.createTempFile("oai", ".txt")
    tempFile.deleteOnExit()
    cacheTempFile(tempFile)
    tempFileToRdd(tempFile)
  }

  private[refactor] def tempFileToRdd(tempFile: File): RDD[Row] = {
    val csvRdd = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .load("file://" + tempFile.getAbsolutePath)
      .rdd

    val eitherRdd = csvRdd.map(handleCsvRow)
    val pagesEitherRdd = eitherRdd.flatMap(
      oaiMethods.parsePageIntoRecords(_, oaiConfiguration.removeDeleted)
    )
    pagesEitherRdd.map(OaiRelation.convertToOutputRow)
  }

  private[refactor] def handleCsvRow(row: Row): OaiPage =
    row.toSeq match {
      case Seq("page", page: String, _) => OaiPage(page)
      case _ =>
        throw new RuntimeException(
          "Don't know how to handle row " + row.mkString(",")
        )
    }

  private[refactor] def cacheTempFile(tempFile: File): Unit = {
    //
    def t(n: Object*) = n

    val writer = new CSVWriter(
      new FileWriter(tempFile),
      ',',
      CSVWriter.DEFAULT_QUOTE_CHARACTER,
      '\\'
    )

    try {
      for (page <- oaiMethods.listAllRecordPages()) {
        val line = t(pageToArray(page): _*)
          .filter(o => o.isInstanceOf[String])
          .map(l => l.toString)
          .toArray
          .padTo(3, "")
        writer.writeNext(line)
      }
    } finally {
      writer.close()
    }
  }

  private[refactor] def pageToArray(
      page: OaiPage
  ): Seq[String] = Seq("page", page.page.replaceAll("(\r\n)|\r|\n", " "), "")

}
