package dpla.ingestion3.harvesters.oai

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

class DefaultSource extends RelationProvider {

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

    new OaiRelation(endpoint, verb, metadataPrefix, harvestAllSets, setlist, blacklist)(sqlContext)
  }

  def getEndpoint(parameters: Map[String, String]): String = {
    parameters.get("path") match {
      case Some(x) => x
      case None => throwMissingArgException("endpoint")
    }
  }

  def getVerb(parameters: Map[String, String]): String = {
    parameters.get("verb") match {
      case Some(x) => x
      case None => throwMissingArgException("verb")
    }
  }

  def getHarvestAllSets(parameters: Map[String, String]): Boolean = {
    parameters.get("harvestAllSets") match {
      case Some(x) => {
        x.toLowerCase match {
          case("true") => true
          case("false") => false
          case (_) => throwUnrecognizedArgException(x)
        }
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
  def parseSets(sets: String): Array[String] = {
    sets.split(",").map(_.trim)
  }

  def throwMissingArgException(arg: String) = {
    val msg = s"Missing argument: $arg"
    throw new IllegalArgumentException(msg)
  }

  def throwUnrecognizedArgException(arg: String) = {
    val msg = s"Unrecognized argument: $arg"
    throw new IllegalArgumentException(msg)
  }
}
