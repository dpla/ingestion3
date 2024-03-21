package dpla.ingestion3.harvesters.oai.refactor

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

/** Entry point for the data source. Uses OaiConfiguration and the parameters to
  * decide which type of harvest to run, and runs it.
  */

class DefaultSource extends RelationProvider {

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
  ): OaiRelation = {

    val config = OaiConfiguration(parameters)
    val oaiMethods = new OaiProtocol(config)
    OaiRelation.getRelation(oaiMethods, config, sqlContext)
  }
}
