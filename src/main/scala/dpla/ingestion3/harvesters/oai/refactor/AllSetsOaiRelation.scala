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
//    sqlContext.sparkContext
//      .parallelize[String](oaiMethods.listAllSets.toSeq)
//      .flatMap(set => oaiMethods.listAllRecordPagesForSet(set))
//      .flatMap(oaiMethods.parsePageIntoRecords(_)) //TODO

  }
}
