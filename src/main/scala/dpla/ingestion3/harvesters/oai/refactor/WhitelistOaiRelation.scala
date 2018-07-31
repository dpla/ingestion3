package dpla.ingestion3.harvesters.oai.refactor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/**
  * OaiRelation for harvests that want to havest all sets *except* those specified.
  *
  * @param oaiMethods Implementation of the OaiMethods trait.
  * @param sqlContext Spark sqlContext.
  */
class WhitelistOaiRelation(oaiConfiguration: OaiConfiguration, oaiMethods: OaiMethods)
                          (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  override def buildScan(): RDD[Row] = {
    val sparkContext = sqlContext.sparkContext
    val whitelist = sparkContext.parallelize(oaiConfiguration.setlist.getOrElse(Array()))
    val whitelistEithers = whitelist.map(set => Right(OaiSet(set, "")))
    val pages = whitelistEithers.flatMap(oaiMethods.listAllRecordPagesForSet)
    val records = pages.flatMap(oaiMethods.parsePageIntoRecords(_, oaiConfiguration.removeDeleted))
    records.map(OaiRelation.convertToOutputRow)
  }
}
