package dpla.ingestion3.harvesters.oai

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import dpla.ingestion3.harvesters.HarvesterExceptions._
import dpla.ingestion3.utils.HttpUtils

class DefaultSource extends RelationProvider {

  // TODO: Are all of these verbs going to be supported out of the gate?
  // "GetRecord", "ListIdentifiers", "ListMetadataFormats", "Identify"
  val oaiVerbs = List("ListRecords", "ListSets")

  /**
    * The "parameters" argument for createRelation must be of type Map[String, String]
    * @see https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/ml/source/libsvm/DefaultSource.html
    *
    * Exceptions are thrown only when given parameters will result in errors
    * within this package.  OAI requests may return additional errors if the
    * given parameters do not conform with OAI rules.
    */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]) : OaiRelation = {

    // Get required args for all OAI harvests.
    val endpoint: String = getEndpoint(parameters)
    val verb: String = getVerb(parameters)

    // Get optional args for OAI harvests.
    val metadataPrefix: Option[String] = parameters.get("metadataPrefix")

    // Get args that indicate how sets should be handled:
    val harvestAllSets: Boolean = getHarvestAllSets(parameters)
    val setlist: Option[Array[String]] = getSetlist(parameters)
    val blacklist: Option[Array[String]] = getBlacklist(parameters)
    val removeDeleted: Boolean = getRemoveDeleted(parameters)

    new OaiRelation(endpoint, verb, metadataPrefix, harvestAllSets, setlist, blacklist, removeDeleted)(sqlContext)
  }

  def getRemoveDeleted(parameters: Map[String, String]): Boolean =
    parameters.get("removeDeleted") match {
      case Some("true") => true
      case Some("false") => false
      case _ => false
    }

  def getEndpoint(parameters: Map[String, String]): String = {
    val endpoint = parameters.get("endpoint")
    (endpoint, HttpUtils.validateUrl(endpoint.getOrElse(""))) match {
      // An endpoint url was provided and is reachable
      case (Some(url), true) => url
      // An endpoint url was provided but is unreachable
      case (Some(url), false) => throwValidationException(s"OAI endpoint ${url} is not reachable.")
      // No endpoint parameter was provided, it is a redundant validation of those in OaiHarvesterConf
      case (None, false) => throwMissingArgException("endpoint")
      // Something very unexpected
      case _ => throwValidationException(s"Unable to validate the OAI endpoint.")
    }
  }

  /**
    * Validates that a verb parameter was passed in and it is a valid OAI verb
    *
    * @param parameters
    * @return The verb if valid
    * @throws IllegalArgumentException
    */
  def getVerb(parameters: Map[String, String]): String = {
    val verb = parameters.get("verb")
    (verb, oaiVerbs.contains(verb.getOrElse(""))) match {
      // A verb parameter was given and it matches the list of valid OAI verbs
      case (Some(v), true) => v
      // A verb parameter was given but it is not a supported OAI verb
      case (Some(v), false) => throwValidationException(s"${v} is not a valid or currently supported OAI verb")
      // A verb was not provided, it is a redundant validation of those in OaiHarvesterConf
      case (None, false) => throwMissingArgException("verb")
      // Something very unexpected
      case _ => throwValidationException("Unable to validate OAI verb.")
    }
  }

  def getHarvestAllSets(parameters: Map[String, String]): Boolean = {
    parameters.get("harvestAllSets") match {
      case Some(x) =>
        x.toLowerCase match {
          case "true" => true
          case "false" => false
          case _ => throwUnrecognizedArgException(x)
        }
      case _ => false
    }
  }

  def getSetlist(parameters: Map[String, String]): Option[Array[String]] = {
    parameters.get("setlist") match {
      case Some(x) => Some(parseSets(x))
      case _ => None
    }
  }

  def getBlacklist(parameters: Map[String, String]): Option[Array[String]] = {
    parameters.get("blacklist") match {
      case Some(x) => Some(parseSets(x))
      case _ => None
    }
  }

  // Converts a comma-separated String of sets to an Array of sets.
  def parseSets(sets: String): Array[String] = sets.split(",").map(_.trim)
}
