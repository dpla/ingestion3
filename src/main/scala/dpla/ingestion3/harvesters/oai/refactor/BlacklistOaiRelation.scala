package dpla.ingestion3.harvesters.oai.refactor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/** OaiRelation for harvests that want to havest all sets *except* those
  * specified.
  *
  * @param oaiMethods
  *   Implementation of the OaiMethods trait.
  * @param sqlContext
  *   Spark sqlContext.
  */
class BlacklistOaiRelation(
    oaiConfiguration: OaiConfiguration,
    oaiMethods: OaiMethods
)(@transient override val sqlContext: SQLContext)
    extends OaiRelation {
  override def buildScan(): RDD[Row] = {
    val sparkContext = sqlContext.sparkContext
    val blacklist = oaiConfiguration.blacklist.getOrElse(Array()).toSet
    val originalSets =
      oaiMethods.listAllSetPages().iterator.flatMap(oaiMethods.parsePageIntoSets)
    val nonBlacklistedSets = originalSets.iterator.filter(set => !blacklist.contains(set.id))
    val sets = sparkContext.parallelize(nonBlacklistedSets.toSeq)
    val pages = sets.flatMap(oaiMethods.listAllRecordPagesForSet)
    val records = pages.flatMap(
      oaiMethods.parsePageIntoRecords(_, oaiConfiguration.removeDeleted)
    )
    records.map(OaiRelation.convertToOutputRow)
  }
}
