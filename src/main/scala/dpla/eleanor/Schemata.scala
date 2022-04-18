package dpla.eleanor
import org.apache.spark.sql.{Encoder, Encoders}
import java.sql.Timestamp

import dpla.eleanor.Schemata.MetadataType
import dpla.ingestion3.model.OreAggregation

object Schemata {

  sealed case class MimeType(mimeType: String) {
    override def toString: String = mimeType
  }

  object MimeType {
    object Epub extends MimeType("application/epub+zip")
    object Pdf extends MimeType("application/pdf")
    object Mobi extends MimeType("application/x-mobipocket-ebook")
  }

  sealed case class SourceUri(uri: String) {
    override def toString: String = uri
  }

  object SourceUri {
    object UnglueIt extends SourceUri("http://unglue.it")
    object StandardEbooks extends SourceUri("http://standardebooks.org")
    object Feedbooks extends SourceUri("http://feedbooks.com")
    object Gutenberg extends SourceUri("http://gutenberg.org")
    object Gpo extends SourceUri("http://gpo.gov")
  }

  sealed case class MetadataType(metadataType: String) {
    override def toString: String = metadataType
  }

  object MetadataType {
    object Opds1 extends MetadataType("opds1")
    object Opds2 extends MetadataType("opds2")
    object Marc extends MetadataType("marc")
    object Rdf extends MetadataType("rdf")
  }

  object Implicits {
    implicit def harvestDataEncoder: Encoder[HarvestData] = Encoders.product[HarvestData]
    implicit def mappedDataEncoder: Encoder[MappedData] = Encoders.product[MappedData]
  }

  case class Payload(
                    timestamp: Timestamp = null,
                    url: String = null,
                    filename: String = null,
                    data: Array[Byte] = null,
                    mimeType: String = null,
                    sha512: String = null,
                    size: Long = -1,
                    cid: String = null
                    )

  case class HarvestData(
                    sourceUri: String,
                    id: String,
                    timestamp: Timestamp,
                    metadataType: MetadataType,
                    metadata: Array[Byte],
                    payloads: Seq[Payload]
                    )

  case class MappedData(
                   // dpla ebook id
                   id: String,

                   sourceUri: String,
                   timestamp: Timestamp,
                   itemUri: String,
                   providerName: String,

                   originalRecord: Array[Byte],
                   payloads: Seq[Payload],

                   // record metadata
                   title: Seq[String] = Seq(),
                   author: Seq[String] = Seq(),
                   subtitle: Seq[String] = Seq(),
                   language: Seq[String] = Seq(),
                   medium: Seq[String] = Seq(),
                   publisher: Seq[String] = Seq(),
                   publicationDate: Seq[String] = Seq(),
                   summary: Seq[String] = Seq(),
                   genre: Seq[String] = Seq()
                   )


  case class IndexData(
                        // dpla ebook id
                        id: String,

                        sourceUri: String,
                        itemUri: String,
                        providerName: String,

                        payloadUri: Seq[String],

                        title: Seq[String] = Seq(),
                        author: Seq[String] = Seq(),
                        subtitle: Seq[String] = Seq(),
                        language: Seq[String] = Seq(),
                        medium: Seq[String] = Seq(),
                        publisher: Seq[String] = Seq(),
                        publicationDate: Seq[String] = Seq(),
                        summary: Seq[String] = Seq(),
                        genre: Seq[String] = Seq()
                      )


  // Replaces MappedData
  case class Ebook(
                    oreAggregation: OreAggregation,
                    payload: Seq[Payload]
                  )

}

case class HarvestStatics(
                           sourceUri: String,
                           timestamp: Timestamp,
                           metadataType: MetadataType
                         )



