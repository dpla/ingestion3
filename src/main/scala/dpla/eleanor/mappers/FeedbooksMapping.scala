package dpla.eleanor.mappers

import java.io.ByteArrayInputStream

import dpla.eleanor.Schemata.{HarvestData, MappedData}

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}


object FeedbooksMapping extends IdMinter {

  lazy val providerName: String = "Feedbooks"

  def id(xml: Elem): String = Some((xml \ "id").text.trim).getOrElse(throw new RuntimeException("Missing required ID"))

  def author(xml: Elem): Seq[String] = (xml \ "author" \ "name").map(_.text)

  def genre(xml: Elem): Seq[String] = (xml \ "category").map(node => node \@ "label")

  def language(xml: Elem): Seq[String] = (xml \ "language").map(_.text)

  def summary(xml: Elem): Seq[String] = (xml \ "summary").map(_.text)

  def title(xml: Elem): Seq[String] = (xml \ "title").map(_.text)

  def uri(xml: Elem): Seq[String] = (xml \ "id").map(_.text)

  def publicationDate(xml: Elem): Seq[String] = (xml \ "issued").map(_.text)

  def map(feedbooks: HarvestData): Option[MappedData] = {
    val xml = Try { XML.load(new ByteArrayInputStream(feedbooks.metadata)) } match {
      case Success(x) => x
      case Failure(_) =>
        println(s"Unable to load record XML metadata for ${feedbooks.id}")
        return None
    }

    // If missing URI log error and return None
    val uri = StandardEbooksMapping.uri(xml).headOption match {
      case Some(t) => t
      case None =>
        println(s"Missing required sourceUri for ${feedbooks.id}")
        return None
    }

    Some(
      MappedData(
        id = mintDplaId(id(xml), providerName),
        sourceUri = feedbooks.sourceUri,
        timestamp = feedbooks.timestamp,
        itemUri = uri,
        providerName = providerName,
        originalRecord = feedbooks.metadata,
        payloads = feedbooks.payloads,

        // record metadata
        title = FeedbooksMapping.title(xml),
        author = FeedbooksMapping.author(xml),
        language = FeedbooksMapping.language(xml),
        summary = FeedbooksMapping.summary(xml),
        genre = FeedbooksMapping.genre(xml),
        publicationDate = FeedbooksMapping.publicationDate(xml)
      )
    )
  }
}
