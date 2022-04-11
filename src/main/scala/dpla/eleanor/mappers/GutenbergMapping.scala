package dpla.eleanor.mappers

import java.io.ByteArrayInputStream

import dpla.eleanor.Schemata.{HarvestData, MappedData}

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}

object GutenbergMapping extends IdMinter {

  lazy val providerName = "Project Gutenberg"

  lazy val uriBase = "https://www.gutenberg.org/"

  def id(xml: Elem): String = Option((xml \ "ebook" \@ s"{${xml.getNamespace("rdf")}}about").trim)
    .getOrElse(throw new RuntimeException("Missing required ID"))

  def author(xml: Elem): Seq[String] = (xml \ "ebook" \ "creator" \ "agent" \ "name").map(_.text)

  def genre(xml: Elem): Seq[String] = (xml \ "ebook" \ "bookshelf" \ "Description" \ "value").map(_.text)

  def language(xml: Elem): Seq[String] = (xml \ "ebook" \ "language" \ "Description" \ "value").map(_.text)

  def summary(xml: Elem): Seq[String] = (xml \ "ebook" \ "description").map(_.text)

  def title(xml: Elem): Seq[String] = (xml \ "ebook" \ "title").map(_.text)

  def uri(xml: Elem): Seq[String] = {

    val path = (xml \ "ebook" \@ s"{${xml.getNamespace("rdf")}}about").trim

    Seq(s"$uriBase$path")
  }

  def map(gutenberg: HarvestData): Option[MappedData] = {
    val xml = Try {
      XML.load(new ByteArrayInputStream(gutenberg.metadata))
    } match {
      case Success(x) => x
      case Failure(_) =>
        println(s"Unable to load record XML metadata for ${gutenberg.id}")
        return None
    }

    // If missing URI log error and return None
    val uri = GutenbergMapping.uri(xml)
      .headOption
    match {
      case Some(t) => t
      case None =>
        println(s"Missing required sourceUri for ${gutenberg.id}")
        return None
    }

    Some(
      MappedData(
        id = mintDplaId(id(xml), providerName),
        sourceUri = gutenberg.sourceUri,
        timestamp = gutenberg.timestamp,
        itemUri = uri,
        providerName = providerName,
        originalRecord = gutenberg.metadata,
        payloads = gutenberg.payloads,

        // record metadata
        title = title(xml),
        author = author(xml),
        language = language(xml),
        summary = summary(xml),
        genre = genre(xml)
        // publicationDate = publicationDate(xml) // no mapping defined
      )
    )
  }
}
