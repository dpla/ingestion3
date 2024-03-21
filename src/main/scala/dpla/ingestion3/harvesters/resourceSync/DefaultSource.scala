package dpla.ingestion3.harvesters.resourceSync
import dpla.ingestion3.harvesters.HarvesterExceptions._
import dpla.ingestion3.utils.HttpUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

class DefaultSource extends RelationProvider {

  val rsCapabilities = List()

  /** The "parameters" argument for createRelation must be of type Map[String,
    * String]
    * @see
    *   https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/ml/source/libsvm/DefaultSource.html
    *
    * Exceptions are thrown only when given parameters will result in errors
    * within this package. ResourceSyn requests may return additional errors if
    * the given parameters do not conform with ResourceSync rules.
    */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
  ): RsRelation = {

    // Get required args for all OAI harvests.
    val endpoint: String = getEndpoint(parameters)

    // Get args that indicate how sets should be handled:
    val harvestAll: Boolean = getHarvestAll(parameters)

    new RsRelation(endpoint, true)(sqlContext)
  }

  def getEndpoint(parameters: Map[String, String]): String = {
    val endpoint = parameters.get("endpoint")
    (endpoint, HttpUtils.validateUrl(endpoint.getOrElse(""))) match {
      // An endpoint url was provided and is reachable
      case (Some(url), true) => url
      // An endpoint url was provided but is unreachable
      case (Some(url), false) =>
        throwValidationException(
          s"ResourceSync endpoint ${url} is not reachable."
        )
      // No endpoint parameter was provided, it is a redundant validation of those in OaiHarvesterConf
      case (None, false) => throwMissingArgException("endpoint")
      // Something very unexpected
      case _ =>
        throwValidationException(
          s"Unable to validate the ResourceSync endpoint."
        )
    }
  }

  def getHarvestAll(parameters: Map[String, String]): Boolean = {
    parameters.get("harvestAll") match {
      case Some(x) =>
        x.toLowerCase match {
          case "true"  => true
          case "false" => false
          case _       => throwUnrecognizedArgException(x)
        }
      case _ => false
    }
  }
}
