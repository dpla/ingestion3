package dpla.ingestion3.enrichments.date

import dpla.ingestion3.model.DplaMapData.ZeroToOne
import dpla.ingestion3.model._

/** */
class DateBuilder {

  /** Attempts to enrich an original date value by identifying begin and end
    * year values from the original string
    * @param originalSourceDate
    *   Un-enriched date value
    * @return
    *   EdmTimeSpan
    */
  def generateBeginEnd(originalSourceDate: ZeroToOne[String]): EdmTimeSpan = {
    originalSourceDate match {
      case None => emptyEdmTimeSpan
      case Some(d) =>
        EdmTimeSpan(
          originalSourceDate = Some(d),
          prefLabel = Some(d),
          begin = createBegin(d.trim),
          end = createEnd(d.trim)
        )
    }
  }

  /** Identifies a begin date
    *
    * @param str
    *   Original date value
    * @return
    *   Option[String]
    */
  def createBegin(str: String): Option[String] = str match {
    case `str` if str.matches("^[0-9]{4}$") => Some(str)
    case `str` if str.matches("^[0-9]{4}\\s*-\\s*[0-9]{4}$") =>
      Option(str.split("-").head.trim)
    case _ => None
  }

  /** Identifies an end date
    *
    * @param str
    *   Original date value
    * @return
    *   Option[String]
    */
  def createEnd(str: String): Option[String] = str match {
    case `str` if str.matches("^[0-9]{4}$") => Some(str)
    case `str` if str.matches("^[0-9]{4}\\s*-\\s*[0-9]{4}$") =>
      Option(str.split("-").last.trim)
    case _ => None
  }
}
