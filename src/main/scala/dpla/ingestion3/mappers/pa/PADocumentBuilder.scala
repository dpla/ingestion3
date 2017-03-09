package dpla.ingestion3.mappers.pa

import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import scala.xml._

class PADocumentBuilder extends XmlExtractionUtils {
  def buildFromXml(rawData: String): PADocument = {

    implicit val xml: NodeSeq = XML.load(rawData)

    PADocument(
        contributors = extractStrings("dc:contributor"),
        coverages = extractStrings("dc:coverage"),
        creators = extractStrings("dc:creator"),
        dates = extractStrings("dc:date"),
        description = extractString("dc:description"),
        formats = extractStrings("dc:type"),
        identifiers = extractStrings("dc:identifier"),
        languages = extractStrings("dc:language"),
        publishers = extractStrings("dc:publisher"),
        relations = extractStrings("dc:relation").drop(1),
        rights = extractStrings("dc:rights"),
        subjects = extractStrings("dc:subject"),
        source = extractString("dc:source"),
        titles = extractStrings("dc:title"),
        types = extractStrings("dc:type")
    )
  }
}
