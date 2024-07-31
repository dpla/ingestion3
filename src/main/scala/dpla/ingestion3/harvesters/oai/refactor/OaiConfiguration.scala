package dpla.ingestion3.harvesters.oai.refactor

import dpla.ingestion3.harvesters.HarvesterExceptions.{
  throwMissingArgException,
  throwUnrecognizedArgException,
  throwValidationException
}
import dpla.ingestion3.utils.HttpUtils

/** Case class that holds the responsibility of interpreting the parameters map
  * from the DefaultSource.
  */
case class OaiConfiguration(parameters: Map[String, String]) {

  def endpoint: String = {
    val endpoint = parameters.get("endpoint")
    (endpoint, HttpUtils.validateUrl(endpoint.getOrElse(""))) match {
      // An endpoint url was provided and is reachable
      case (Some(url), true) => url
      // An endpoint url was provided but is unreachable
      case (Some(url), false) =>
        throwValidationException(s"OAI endpoint $url is not reachable.")
      // No endpoint parameter was provided, it is a redundant validation of those in OaiHarvesterConf
      case (None, false) => throwMissingArgException("endpoint")
      // Something very unexpected
      case _ =>
        throwValidationException(s"Unable to validate the OAI endpoint.")
    }
  }

  def verb: String = {
    val verb = parameters.get("verb")
    (verb, oaiVerbs.contains(verb.getOrElse(""))) match {
      // A verb parameter was given and it matches the list of valid OAI verbs
      case (Some(v), true) => v
      // A verb parameter was given but it is not a supported OAI verb
      case (Some(v), false) =>
        throwValidationException(
          s"$v is not a valid or currently supported OAI verb"
        )
      // A verb was not provided, it is a redundant validation of those in OaiHarvesterConf
      case (None, false) => throwMissingArgException("verb")
      // Something very unexpected
      case _ => throwValidationException("Unable to validate OAI verb.")
    }
  }

  def metadataPrefix: Option[String] = parameters.get("metadataPrefix")

  def sleep: Int = parameters.getOrElse("sleep", "0").toInt

  def harvestAllSets: Boolean = parameters
    .get("harvestAllSets")
    .map(_.toLowerCase) match {
    case Some("true")  => true
    case Some("false") => false
    case None          => false
    case x             => throwUnrecognizedArgException(s"harvestAllSets => $x")
  }

  def setlist: Option[Array[String]] = parameters.get("setlist").map(parseSets)

  def blacklist: Option[Array[String]] =
    parameters.get("blacklist").map(parseSets)

  def removeDeleted: Boolean = parameters.get("removeDeleted") match {
    case Some("true") => true
    case _            => false
  }

  private[this] val oaiVerbs = Set("ListRecords", "ListSets")

  private[this] def parseSets(sets: String): Array[String] =
    sets.split(",").map(_.trim)

  // Ensure that set handlers are only used with the "ListRecords" verb.
  private[this] def validateArguments: Boolean =
    verb == "ListRecords" ||
      Seq(harvestAllSets, setlist, blacklist)
        .forall(x => x == None || x == false)

  require(
    validateArguments,
    "The following can only be used with the 'ListRecords' verb: harvestAllSets, setlist, blacklist"
  )
}
