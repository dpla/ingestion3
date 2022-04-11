package dpla.eleanor.mappers

import java.io.ByteArrayInputStream

import dpla.eleanor.Schemata.{HarvestData, MappedData}

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}

object StandardEbooksMapping extends IdMinter {

  lazy val providerName = "Standard Ebooks"

  def id(xml: Elem): String = Option((xml \ "id").text.trim).getOrElse(throw new RuntimeException("Missing required ID"))

  def author(xml: Elem): Seq[String] = (xml \ "author" \ "name").map(_.text)

  def genre(xml: Elem): Seq[String] = (xml \ "category").map(node => node \@ "term")

  def language(xml: Elem): Seq[String] = (xml \ "language").map(_.text)

  def summary(xml: Elem): Seq[String] = (xml \ "summary").map(_.text)

  def title(xml: Elem): Seq[String] = (xml \ "title").map(_.text)

  def uri(xml: Elem): Seq[String] = (xml \ "id").map(_.text)

  def map(standardEbooks: HarvestData): Option[MappedData] = {
    val xml = Try {
      XML.load(new ByteArrayInputStream(standardEbooks.metadata))
    } match {
      case Success(x) => x
      case Failure(_) =>
        println(s"Unable to load record metadata for ${standardEbooks.id}")
        return None
    }

    // If missing URI throw error
    val uri = StandardEbooksMapping.uri(xml)
      .headOption
    match {
      case Some(t) => t
      case None =>
        println(s"Missing required sourceUri for ${standardEbooks.id}")
        return None
    }

    Some(
      MappedData(
        // id
        id = mintDplaId(id(xml), providerName),

        sourceUri = standardEbooks.sourceUri,
        timestamp = standardEbooks.timestamp,
        itemUri = uri,
        providerName = providerName,
        originalRecord = standardEbooks.metadata,
        payloads = standardEbooks.payloads,

        // record metadata
        title = title(xml),
        author = author(xml),
        language = language(xml),
        summary = summary(xml),
        genre = genre(xml)
      )
    )
  }
}
