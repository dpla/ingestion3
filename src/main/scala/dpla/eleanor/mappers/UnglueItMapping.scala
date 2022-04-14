package dpla.eleanor.mappers

import java.io.ByteArrayInputStream

import dpla.eleanor.Schemata.{HarvestData, MappedData}

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}

object UnglueItMapping extends IdMinter {
  lazy val providerName = "Unglue.it"

  def id(xml: Elem): String = Option((xml \ "id").text.trim).getOrElse(throw new RuntimeException("Missing required ID"))

  def author(xml: Elem): Seq[String] = (xml \ "author" \ "name").map(_.text)

  def genre(xml: Elem): Seq[String] = (xml \ "category").map(node => node \@ "term")

  def language(xml: Elem): Seq[String] = (xml \ "language").map(_.text)

  def summary(xml: Elem): Seq[String] = (xml \ "content").map(_.text)

  def title(xml: Elem): Seq[String] = (xml \ "title").map(_.text)

  def uri(xml: Elem): Seq[String] = (xml \ "id").map(_.text)

  def publicationDate(xml: Elem): Seq[String] = (xml \ "issued").map(_.text)

  def map(unglueit: HarvestData): Option[MappedData] = {
    val xml = Try {
      XML.load(new ByteArrayInputStream(unglueit.metadata))
    } match {
      case Success(x) => x
      case Failure(_) =>
        println(s"Unable to load record XML metadata for ${unglueit.id}")
        return None
    }

    // If missing URI log error and return None
    val uri = (xml \ "id")
      .headOption
    match {
      case Some(t) => t.text
      case None =>
        println(s"Missing required sourceUri for ${unglueit.id}")
        return None
    }

    Some(
      MappedData(
        // id
        id = mintDplaId(id(xml), providerName),

        sourceUri = unglueit.sourceUri,
        timestamp = unglueit.timestamp,
        itemUri = uri,
        providerName = providerName,
        originalRecord = unglueit.metadata,
        payloads = unglueit.payloads,

        // record metadata
        title = title(xml),
        author = author(xml),
        language = language(xml),
        summary = summary(xml),
        genre = genre(xml),
        publicationDate = publicationDate(xml)
      )
    )
  }
}
