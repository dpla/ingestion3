package dpla.ingestion3.harvesters.pss

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

class DefaultSource extends RelationProvider {

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
  ): PssRelation = {

    new PssRelation(parameters)(sqlContext)
  }
}
