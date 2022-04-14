package dpla.eleanor

import dpla.eleanor.Schemata.{Ebook, HarvestData, Payload}
import dpla.eleanor.profiles.{EbookProfile, EbookProviderRegistry}
import dpla.ingestion3.executors.EbookMap
import dpla.ingestion3.mappers.utils.{Document, Mapping, XmlMapping}
import dpla.ingestion3.messages.{IngestMessage, IngestMessageTemplates, MessageCollector}
import dpla.ingestion3.model.DplaMapData.ZeroToMany
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, _}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

/**
  * Map Payload data
  *
  * TODO -- should contain validations for payload information (see CH Mapper trait (e.g. validateEdmRights etc.)
  *
  * @tparam T
  * @tparam E
  */
trait PayloadMapper[T, +E] extends IngestMessageTemplates {
  implicit val formats: DefaultFormats.type = DefaultFormats
  def map(document: Document[T], mapping: Mapping[T]): ZeroToMany[Payload]

  /**
    *
    * @param payload
    * @return
    */
  def validatePayload(payload: Payload) = ???
}

/**
  * Implementation of Payload mapper
  */
class XmlPayloadMapper extends PayloadMapper[NodeSeq, XmlMapping] {
  override def map(document: Document[NodeSeq], mapping: Mapping[NodeSeq]): ZeroToMany[Payload] = {
    implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]

    val providerId = Try {
      mapping.sidecar(document) \\ "prehashId"
    } match {
      case Success(s) => s.extractOrElse[String]("Unknown")
      case Failure(f) => s"Fatal error - Missing required ID $document"
    }

   Try {
      mapping.payloads(document)
    } match {
      case Success(payload) => payload
      case Failure(f) =>
        msgCollector.add(exception(providerId, f))
        // Return an empty Payload that contains all the messages generated from failed mapping
        // emptyPayload.copy(messages = msgCollector.getAll()) // fixme
       Seq() // fixme see above
    }
  }
}

/**
  * Ebook mapper
  */
object Mapper {

  def execute(spark: SparkSession, harvest: Dataset[HarvestData]): Dataset[Ebook] = {
    val window: WindowSpec = Window.partitionBy("id").orderBy(col("timestamp").desc)
    val dplaEbookMap = new EbookMap()

    import spark.implicits._

    harvest
      .withColumn("rn", row_number.over(window))
      .where(col("rn") === 1).drop("rn") // get most recent versions of each record ID and drop all others
      .select("metadata", "sourceUri")
      .map(harvestRecord => {
        val metadata = new String(harvestRecord.get(0).asInstanceOf[Array[Byte]], "utf-8") // convert Array[Byte] to String
        val shortName =  harvestRecord.getString(1) // Source.uri as providerProfile short name
        // lookup extractor at runtime because multiple providers could be mapped at once
        dplaEbookMap.map(metadata, getExtractorClass(shortName))
      })
  }

  /**
    * Look up provider profile in Ebooks Provider Registry
    * @param shortName
    * @return
    */
  def getExtractorClass(shortName: String): EbookProfile[_ >: NodeSeq with JValue] =
    EbookProviderRegistry.lookupProfile(shortName) match {
      case Success(extClass) => extClass
      case Failure(e) => throw new RuntimeException(s"Unable to load $shortName mapping from CHProviderRegistry")
    }
}

