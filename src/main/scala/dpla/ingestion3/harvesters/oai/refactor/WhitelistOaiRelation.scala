package dpla.ingestion3.harvesters.oai.refactor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class WhitelistOaiRelation(whitelistHarvest: WhitelistHarvest)
                          (@transient oaiMethods: OaiMethods)
                          (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  override def buildScan(): RDD[Row] = ???
}
