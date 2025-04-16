package dpla.ingestion3.mappers.providers

import dpla.ingestion3.executors.DplaMap
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model.{DplaPlace, EdmAgent, EdmTimeSpan, EdmWebResource, LiteralOrUri, SkosConcept, URI, nameOnlyAgent, nameOnlyConcept, nameOnlyPlace, stringOnlyTimeSpan, stringOnlyWebResource}
import dpla.ingestion3.utils.{CHProviderRegistry, Utils}
import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import org.json4s.JValue

import scala.xml._
import org.json4s.JsonDSL._

class IthakaMapping
    extends XmlMapping
    with XmlExtractor
    with IngestMessageTemplates {

  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some(
    "artstor"
  ) // TODO: does this actually help?

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier").map(_.trim)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    EdmAgent(
      uri = Some(URI("http://dp.la/api/contributor/artstor")),
      name = Some("Artstor") // TODO JSTOR? Something else
    )

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    Seq(
      EdmAgent(
        uri = Some(URI("http://dp.la/api/contributor/ithaka")),
        name = Some("Ithaka") // TODO JSTOR? Something else
      )
    )

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(
      data
    ))

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "dc" \ "contributor")
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";"))
      .flatMap(_.splitAtDelimiter(":"))
      .filter(x => x.nonEmpty && !x.equalsIgnoreCase("unknown"))
      .map(nameOnlyAgent)

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "dc" \ "publisher")
      .filter(x => x.nonEmpty && !x.equalsIgnoreCase("unknown"))
      .map(nameOnlyAgent)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(data \ "metadata" \ "dc" \ "title")

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "dc" \ "creator")
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";"))
      .filter(x => x.nonEmpty && !x.equalsIgnoreCase("unknown"))
      .map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \ "dc" \ "subject")
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";"))
      .filter(_.nonEmpty)
      .map(nameOnlyConcept)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "dc" \ "description")
      .map(_.trim)
      .filter(_.nonEmpty)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "metadata" \ "dc" \ "date")
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";"))
      .filter(_.nonEmpty)
      .map(stringOnlyTimeSpan)

  private def typeyFormaty(data: Document[NodeSeq]): ZeroToMany[String] =
    (extractStrings(data \ "metadata" \ "dc" \ "type") ++
      extractStrings(data \ "metadata" \ "dc" \ "format"))
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";"))
      .filter(_.nonEmpty)

  // todo make sure these values look ok
  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    typeyFormaty(data)

  // todo make sure these values look ok
  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    typeyFormaty(data).map(_.applyBlockFilter(ExtentIdentificationList.termList))

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    typeyFormaty(data).map(_.applyAllowFilter(ExtentIdentificationList.termList))

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "dc" \ "identifier")
      .flatMap(_.splitAtDelimiter(";"))
      .filter(x =>
        x.nonEmpty &&
          !x.startsWith("Thumbnail:") &&
          !x.startsWith("Medias:") &&
          !x.startsWith("FullSize:") &&
          !x.startsWith("ADLImageViewer:") &&
          !x.startsWith("SSCImageViewer:")
      ) // todo ensure this is the correct list of prefixes to filter out

  // TODO ADLImageViewer or SSCImageViewer or both?
  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \ "dc" \ "identifier")
      .map(x => x.trim)
      .filter(x =>
        x.startsWith("ADLImageViewer:") || x.startsWith("SSCImageViewer:")
      )
      .map(x => x.substring(x.indexOf(":") + 1))
      .map(stringOnlyWebResource)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractStrings(data \ "metadata" \ "dc" \ "relation")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(LiteralOrUri(_, isUri = false))

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \ "dc" \ "language")
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    extractStrings(data \ "metadata" \ "dc" \ "coverage")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(nameOnlyPlace)

  override def rights(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "dc" \ "rights")
      .map(_.trim)
      .filter(_.nonEmpty)

}

object IthakaMapping extends IthakaMapping {
  def main(args: Array[String]): Unit = {

    val dplaMap = new DplaMap()
    val extractorClass = CHProviderRegistry.lookupProfile("ithaka").get
    val oreAggregation = dplaMap.map(record.toString(), extractorClass)
    oreAggregation.sourceResource.title.foreach(println)
  }

  val record: Elem =
    <record>
      <header xmlns="http://www.openarchives.org/OAI/2.0/"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <identifier>2841861</identifier>
        <datestamp>2025-02-13T14:49:13Z</datestamp>
      </header>
      <metadata xmlns="http://www.openarchives.org/OAI/2.0/"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <oai_dc:dc
        xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd"
        xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
        xmlns:dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
          <oai_dc:title> Hooigracht. 's GRAVENHAGE. [Hooigracht (canal), The Hague]; verso: Grand
            Bazar de la Paix. [divided back, no message] </oai_dc:title>
          <oai_dc:title>overall</oai_dc:title>
          <oai_dc:title>recto</oai_dc:title>
          <oai_dc:creator> Probably Grand Bazar de la Paix (publisher, Dutch, act. ca.1907-1914) </oai_dc:creator>
          <oai_dc:subject> Postcards; Netherlands; Holland (historical region); Architecture;
            Hague (The) (Zuid-Holland, Netherlands); Canals </oai_dc:subject>
          <oai_dc:date>ca.1907-1914 (publication date)</oai_dc:date>
          <oai_dc:type>Picture postcard</oai_dc:type>
          <oai_dc:format>cardstock|paper</oai_dc:format>
          <oai_dc:format>9 x 14 cm (3.54 x 5.51 inches) approximately</oai_dc:format>
          <oai_dc:identifier>Box 12.54-10</oai_dc:identifier>
          <oai_dc:identifier>534724</oai_dc:identifier>
          <oai_dc:identifier> Thumbnail:https://forum.jstor.org/oai/thumbnail?id=2841861 </oai_dc:identifier>
          <oai_dc:identifier> Medias:https://forum.jstor.org/oai/medias/2841861 </oai_dc:identifier>
          <oai_dc:identifier>
            FullSize:https://forum.jstor.org/assets/2841861/representation/size/9 </oai_dc:identifier>
          <oai_dc:identifier>
            ADLImageViewer:https://www.jstor.org/stable/10.2307/community.2841861 </oai_dc:identifier>
          <oai_dc:identifier>
            SSCImageViewer:https://www.jstor.org/stable/10.2307/community.2841861 </oai_dc:identifier>
          <oai_dc:source>Trinity College Library</oai_dc:source>
          <oai_dc:coverage>Dutch</oai_dc:coverage>
          <oai_dc:coverage> Trinity College, Watkinson Library (Hartford, Connecticut, USA) </oai_dc:coverage>
          <oai_dc:rights> This digital collection and its contents are made available by Trinity
            College Library for limited noncommercial, educational, and personal use only. For
            other uses, or for additional information regarding the collection, contact the
            staff of Watkinson Library, Trinity College, Hartford, CT 06106. </oai_dc:rights>
        </oai_dc:dc>
      </metadata>
      <about>
        <request verb="ListRecords" metadataPrefix="oai_dc" set="580" />
        <resumptionToken> set%3D580%26metadataPrefix%3Doai_dc%26batch_size%3D11%26cursor%3D100 </resumptionToken>
        <responseDate>2025-04-09T18:58:37.104Z</responseDate>
      </about>
    </record>
}
