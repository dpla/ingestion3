package dpla.ingestion3.harvesters.oai.refactor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}


/**
  * OaiRelation for harvests that want to havest all sets *except* those specified.
  *
  * @param oaiMethods Implementation of the OaiMethods trait.
  * @param sqlContext Spark sqlContext.
  */
class BlacklistOaiRelation(oaiConfiguration: OaiConfiguration, @transient oaiMethods: OaiMethods)
                          (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  override def buildScan(): RDD[Row] = ???
}
