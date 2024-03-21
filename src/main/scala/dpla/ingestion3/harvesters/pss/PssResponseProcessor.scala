package dpla.ingestion3.harvesters.pss

import org.apache.log4j.LogManager
import org.json4s.{JValue, JArray, DefaultFormats}
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

/*
 * This class parses primary source set response data during harvest.
 */
object PssResponseProcessor {
  private[this] val logger = LogManager.getLogger("OaiHarvester")

  // Setting formats allows you to parse (ie. extract) Strings from JValues.
  implicit val formats: DefaultFormats.type = DefaultFormats

  // Parse the set endpoints (ie. urls) from an ItemList of all sets.
  def getSetEndpoints(sets: String): List[String] = {
    val json: JValue = parse(sets)
    val endpoints: List[JValue] = (json \\ "itemListElement" \\ "@id").children
    endpoints.map(j => j.extract[String].concat(".json"))
  }

  // Parse the part endpoints (ie. urls) from a JSON set.
  // Return endpoints with .json extensions.
  def getPartEndpoints(set: String): List[String] = {
    val json: JValue = parse(set)
    val endpoints: List[JValue] = (json \\ "hasPart" \\ "@id").children
    endpoints.map(j => j.extract[String].concat(".json"))
  }

  // Combine a set with its components parts.
  def combineSetAndParts(set: String, parts: List[String]): String = {
    val jsonSet: JValue = parse(set)
    val jsonParts: JArray = JArray(parts.map(part => parse(part)))
    val cleanJsonParts: JValue = cleanParts(jsonParts)

    // The set has minimal metadata for its component parts in "hasPart".
    // Replace this minimal metadata with the full parts metadata.
    val setWithParts: JValue = jsonSet.transformField { case ("hasPart", _) =>
      ("hasPart", cleanJsonParts)
    }
    // Convert JValue to String.
    Serialization.write(setWithParts)
  }

  // Remove @context from each part.
  // Identical @context values are present in the set.
  def cleanParts(parts: JArray): JValue = {
    parts.removeField {
      case ("@context", _) => true
      case _               => false
    }
  }

  // Parse the set ID (ie. slug) from its endpoint (ie. url).
  def getSetId(endpoint: String): String = {
    val id = endpoint.split("/").lastOption.getOrElse {
      throw new RuntimeException("""Could not get set ID from: """ + endpoint)
    }
    id.stripSuffix(".json")
  }
}
