package dpla.ingestion3.harvesters.oai

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

class DefaultSource extends RelationProvider {
  
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]) : OaiRelation = {

    val endpoint = parameters("path")
    val metadataPrefix = parameters("metadataPrefix")
    val verb = parameters("verb")
    
    new OaiRelation(endpoint, metadataPrefix, verb)(sqlContext)
  }
}
