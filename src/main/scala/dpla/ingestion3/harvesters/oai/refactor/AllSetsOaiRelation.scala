package dpla.ingestion3.harvesters.oai.refactor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class AllSetsOaiRelation(allSetsHarvest: AllSetsHarvest)
                        (@transient oaiMethods: OaiMethods)
                        (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  override def buildScan(): RDD[Row] = {
    sqlContext.sparkContext
      .parallelize[String](oaiMethods.listAllSets.toSeq)
      .flatMap(set => oaiMethods.listAllRecordPagesForSet(set))
      .flatMap(oaiMethods.parsePageIntoRecords(_)) //TODO

  }
}
