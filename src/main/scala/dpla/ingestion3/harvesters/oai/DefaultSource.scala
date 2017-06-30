package dpla.ingestion3.harvesters.oai

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

class DefaultSource extends RelationProvider {

  // The parameters must be of type Map[String, String]
  // @see https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/ml/source/libsvm/DefaultSource.html
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]) : OaiRelation = {

    new OaiRelation(parameters)(sqlContext)
  }
}
