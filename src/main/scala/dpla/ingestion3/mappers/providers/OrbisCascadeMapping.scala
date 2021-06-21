package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class OrbisCascadeMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {
  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "orbis-cascade"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] = {
    val xml: Elem  = data.get.asInstanceOf[Elem]
    xml.attribute(xml.getNamespace("rdf"), "about")
      .getOrElse(throw new Exception("Missing required record ID"))
      .flatMap(extractStrings(_))
      .headOption
  }

  // SourceResource mapping
    override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
      extractStrings(data \\ "SourceResource" \ "contributor")
        .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \\ "SourceResource" \ "contributor")
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = {
    val dateEarly = extractStrings(data \\ "SourceResource" \ "date" \ "TimeSpan" \ "begin")
    val dateLate = extractStrings(data \\ "SourceResource" \ "date" \ "TimeSpan" \ "end")

    if(dateEarly.length == dateLate.length) {
      dateEarly.zip(dateLate).map {
        case (begin: String, end: String) =>
          EdmTimeSpan(
            originalSourceDate = Some(s"$begin-$end"),
            begin = Some(begin),
            end = Some(end)
          )
      }
    } else {
      Seq(emptyEdmTimeSpan)
    }
  }

  // done
  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "SourceResource" \ "description")

  // done
  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \\ "SourceResource" \ "language")
      .map(nameOnlyConcept)

  // done
  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \\ "SourceResource" \ "spatial")
      .map(nameOnlyPlace)

  // done
  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \\ "SourceResource" \ "subject")
      .map(nameOnlyConcept)

  // done
  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "SourceResource" \ "title")

  // done
  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "SourceResource" \ "hasType")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    // <edm:dataProvider rdf:resource="http://harvester.orbiscascade.org/agency/wauar"/>
    val lookup = Map[String, String](
      // For posterity.
      // Although wauar (University of Washington) data was submitted to DPLA through Orbis-Cascade that data was for
      // testing purposes only and it was not intended to replace the UW content submitted previously as a content hub
      // and they have requested to not be ingested via Orbis-Cascade. This may change at a future point when UW needs
      // to have content updated in DPLA but it is not this day.
      // "http://harvester.orbiscascade.org/agency/wauar"->"University of Washington Libraries, Special Collections",

      // Canonical list of mappings
      // https://docs.google.com/spreadsheets/d/17YZEQRtxdgttRCW6NsCWQ2d9qI9L4IrqzXcPEr6mg18/edit#gid=0
      
      "http://archiveswest.orbiscascade.org/contact#idu"->"University of Idaho Library, Special Collections and Archives",
      "http://harvester.orbiscascade.org/agency/ormcl"->"Linfield College",
      "http://archiveswest.orbiscascade.org/contact#orkt"->"Oregon Institute of Technology Libraries, Shaw Historical Library",
      "http://archiveswest.orbiscascade.org/contact#orpl"->"Lewis & Clark College, Special Collections and Archives",
      "http://archiveswest.orbiscascade.org/contact#waps"->"Washington State University Libraries, Manuscripts, Archives, and Special Collections",
      "http://archiveswest.orbiscascade.org/contact#watu"->"University of Puget Sound, Archives & Special Collections",
      "http://archiveswest.orbiscascade.org/contact#orngf"->"George Fox University Archives",
      "http://archiveswest.orbiscascade.org/contact#wachene"->"Eastern Washington University",
      "http://archiveswest.orbiscascade.org/contact#orsaw"->"Willamette University Archives and Special Collections",
      "http://harvester.orbiscascade.org/agency/orpr"->"Reed College",
      "http://archiveswest.orbiscascade.org/contact#ormonw"->"Western Oregon University Archives",
      "http://archiveswest.orbiscascade.org/contact#orcs"->"Oregon State University Libraries, Special Collections and Archives Research Center",
      "http://archiveswest.orbiscascade.org/contact#waspc"->"Seattle Pacific University",
      "http://harvester.orbiscascade.org/agency/orashs"->"Southern Oregon University, Hannon Library",
      "http://harvester.orbiscascade.org/agency/orpu"->"University of Portland",
      "http://archiveswest.orbiscascade.org/contact#oru"->"University of Oregon Libraries, Special Collections and University Archives"
    )


    val xml: Elem = (data \ "dataProvider").headOption.getOrElse(Node).asInstanceOf[Elem]
    val dataProvider = xml.attribute(xml.getNamespace("rdf"), "resource").getOrElse(Seq())

    dataProvider
      .flatMap(extractStrings)
      .flatMap(uri => lookup.get(uri))
      .map(nameOnlyAgent)
  }
  
  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] = {
    val xml: Elem = (data \ "rights").headOption.getOrElse(Node).asInstanceOf[Elem]
    val rights = xml.attribute(xml.getNamespace("rdf"), "resource").getOrElse(Seq())

    rights
      .flatMap(extractStrings)
      .map(URI)
  }

  override def rights(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "SourceResource" \ "rights")

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    val srAbout = (data \\ "SourceResource").headOption match {
      case Some(node) =>
        val elem = node.asInstanceOf[Elem]
        elem
          .attribute(elem.getNamespace("rdf"), "about")
          .getOrElse(Seq())
          .flatMap(extractStrings(_))
      case None => Seq()
    }
    
    srAbout.map(stringOnlyWebResource)
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    val preview = (data \ "preview" \ "WebResource").headOption match {
      case Some(node) =>
        val elem = node.asInstanceOf[Elem]
        elem
          .attribute(elem.getNamespace("rdf"), "about")
          .getOrElse(Seq())
          .flatMap(extractStrings(_))
      case None => Seq()
    }

    preview.map(stringOnlyWebResource)
  }

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Orbis Cascade Alliance"),
    uri = Some(URI("http://dp.la/api/contributor/orbiscascade"))
  )
}
