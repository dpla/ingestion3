package dpla.ingestion3.harvesters.oai.refactor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/**
  * OaiRelation for harvests that capture all available sets.
  *
  * @param oaiMethods Implementation of the OaiMethods trait.
  * @param sqlContext Spark sqlContext.
  */

class AllSetsOaiRelation(oaiConfiguration: OaiConfiguration, @transient oaiMethods: OaiMethods)
                        (@transient override val sqlContext: SQLContext)
  extends OaiRelation {

  override def buildScan(): RDD[Row] = {
    val sets = sqlContext.sparkContext.parallelize(oaiMethods.listAllSets().toSeq)
    val pages = sets.flatMap(oaiMethods.listAllRecordPagesForSet)
    val records = pages.flatMap(oaiMethods.parsePageIntoRecords)
    records.map(OaiRelation.convertToOutputRow)
  }
}
