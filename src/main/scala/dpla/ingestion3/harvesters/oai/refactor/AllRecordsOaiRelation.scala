package dpla.ingestion3.harvesters.oai.refactor

import java.io.{File, FileWriter}

import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class AllRecordsOaiRelation(allRecordsHarvest: AllRecordsHarvest)
                           (@transient oaiProtocol: OaiMethods)
                           (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  //list all the record pages, flat map to records
  override def buildScan(): RDD[Row] = {

    val tempFile = File.createTempFile("oai", ".txt")
    val writer = new FileWriter(tempFile)

    try {
      for (page <- oaiProtocol.listAllRecordPages)
        writer.write(page.replaceAll("\n", " "))
      //todo error handling

    } finally {
      IOUtils.closeQuietly(writer)
    }

    sqlContext.read.text(tempFile.getAbsolutePath)
      .flatMap(row => oaiProtocol.parsePageIntoRecords(row.getString(0)))
      .map(Row(None, _, None))
      .rdd
  }
}
