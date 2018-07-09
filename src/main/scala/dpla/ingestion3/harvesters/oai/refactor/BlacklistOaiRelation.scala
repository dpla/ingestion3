package dpla.ingestion3.harvesters.oai.refactor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}


/**
  * OaiRelation for harvests that want to havest all sets *except* those specified.
  *
  * @param oaiMethods Implementation of the OaiMethods trait.
  * @param sqlContext Spark sqlContext.
  */
class BlacklistOaiRelation(oaiConfiguration: OaiConfiguration, oaiMethods: OaiMethods)
                          (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  override def buildScan(): RDD[Row] = {
    val sparkContext = sqlContext.sparkContext
    val setPages = sparkContext.parallelize(oaiMethods.listAllSetPages().toSeq)
    val sets = setPages.flatMap(oaiMethods.parsePageIntoSets)
    val blacklist = sparkContext.broadcast(oaiConfiguration.blacklist.getOrElse(Array()).toSet)
    val blacklistedSets = sets.filter {
      case Right(OaiSet(set, _)) => !blacklist.value.contains(set)
      case _ => true
    }
    val pages = blacklistedSets.flatMap(oaiMethods.listAllRecordPagesForSet)
    val records = pages.flatMap(oaiMethods.parsePageIntoRecords(_, oaiConfiguration.removeDeleted))
    records.map(OaiRelation.convertToOutputRow)
  }
}
