package dpla.ingestion3.harvesters.oai.refactor

/**
  * OAI-PMH harvester for DPLA's Ingestion3 system
  */
object OaiResponseProcessor {

  /**
    * Get the resumptionToken from the response
    *
    * @param page String
    *             The String response from an OAI request
    * @return Option[String]
    *         The resumptionToken to fetch the next set of records
    *         or None if no more records can be fetched. An
    *         empty string does not mean all records were successfully
    *         harvested (an error could have occurred when fetching), only
    *         that there are no more records that can be fetched.
    */
  def getResumptionToken(page: String): Option[String] = {
    val pattern = """<resumptionToken.*>(.*)</resumptionToken>""".r
    pattern.findFirstMatchIn(page) match {
      case Some(m) => Some(m.group(1))
      case _ => None
    }
  }
}
