package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.confs.{Ingestion3Conf, i3Conf}
import dpla.ingestion3.harvesters.LocalHarvester
import dpla.ingestion3.harvesters.file.ParsedResult
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.nio.file.{Files, Path}
import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime, ZoneId}

class LocalOaiHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(spark, shortName, conf) {

  private val readerOptions: Map[String, String] = Map(
    "verb" -> conf.harvest.verb,
    "metadataPrefix" -> conf.harvest.metadataPrefix,
    "harvestAllSets" -> conf.harvest.harvestAllSets,
    "setlist" -> conf.harvest.setlist,
    "blacklist" -> conf.harvest.blacklist,
    "endpoint" -> conf.harvest.endpoint,
    "removeDeleted" -> Some("true"),
    "sleep" -> conf.harvest.sleep
  ).collect { case (key, Some(value)) => key -> value }

  private val oaiConfig = OaiConfiguration(readerOptions)
  private val oaiMethods = new OaiProtocol(oaiConfig)
  private val avroWriter = getAvroWriter

  override def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  override def localHarvest(): DataFrame = {

    (oaiConfig.setlist, oaiConfig.harvestAllSets, oaiConfig.blacklist) match {

      case (None, false, Some(blacklist)) =>
        banlistHarvest(blacklist)
      case (Some(setList), false, None) =>
        allowListHarvest(setList)
      case (None, false, None) =>
        allRecordsHarvest()
      case (None, true, None) =>
        allSetsHarvest()
      case _ =>
        throw OaiHarvesterException(
          "Unable to determine harvest type from parameters."
        )
    }

    avroWriter.flush()
    spark.read.format("avro").load(tmpOutStr)
  }

  private def addAboutToDocument(oaiRecord: OaiRecord): OaiRecord = {
    val document = oaiRecord.document
    val info = oaiRecord.info

    val requestElement = <request
      verb={info.verb}
      metadataPrefix={info.metadataPrefix.orNull}
      set={info.set.orNull}
      />

    val resumptionToken = if (info.resumptionToken.isDefined) <resumptionToken>{info.resumptionToken.get}</resumptionToken> else null
    val formatter = DateTimeFormatter.ISO_INSTANT
    val dateString = formatter.format(OffsetDateTime.ofInstant(Instant.ofEpochMilli(info.timestamp), ZoneId.systemDefault()))
    val responseDate = <responseDate>{dateString}</responseDate>

    val documentXml = scala.xml.XML.loadString(document)
    val header = documentXml \ "header"
    val metadata = documentXml \ "metadata"
    val about = <about>{requestElement}{resumptionToken}{responseDate}</about>
    val record = <record>{header}{metadata}{about}</record>
    oaiRecord.copy(document = record.toString)
  }

  private def allowListHarvest(sets: Array[String]): Unit = {
    val unixEpoch = getUnixEpoch
    for {
      set <- sets
      page <- oaiMethods.listAllRecordPagesForSet(set)
      record <- oaiMethods.parsePageIntoRecords(
        page,
        removeDeleted = oaiConfig.removeDeleted()
      )
      fixedRecord = addAboutToDocument(record)
    } writeOut(unixEpoch, ParsedResult(fixedRecord.id, fixedRecord.document))
  }

  private def allSetsHarvest(): Unit = {
    val unixEpoch = getUnixEpoch
    for {
      pageEither <- oaiMethods.listAllSetPages()
      set <- oaiMethods.parsePageIntoSets(pageEither)
      pageEither <- oaiMethods.listAllRecordPagesForSet(set.id)
      record <- oaiMethods.parsePageIntoRecords(
        pageEither,
        removeDeleted = oaiConfig.removeDeleted()
      )
      fixedRecord = addAboutToDocument(record)
    } writeOut(unixEpoch, ParsedResult(fixedRecord.id, fixedRecord.document))
  }

  private def allRecordsHarvest(): Unit = {
    val unixEpoch = getUnixEpoch
    for {
      pageEither <- oaiMethods.listAllRecordPages()
      record <- oaiMethods.parsePageIntoRecords(
        pageEither,
        removeDeleted = oaiConfig.removeDeleted()
      )
      fixedRecord = addAboutToDocument(record)
    } writeOut(unixEpoch, ParsedResult(fixedRecord.id, fixedRecord.document))
  }

  private def banlistHarvest(
      blacklist: Array[String]
  ): Unit = {
    val unixEpoch = getUnixEpoch
    val originalSets = oaiMethods
      .listAllSetPages()
      .iterator
      .flatMap(oaiMethods.parsePageIntoSets)
    for {
      set <- originalSets
      if !blacklist.contains(set.id)
      pageEither <- oaiMethods.listAllRecordPagesForSet(set.id)
      record <- oaiMethods.parsePageIntoRecords(
        pageEither,
        removeDeleted = oaiConfig.removeDeleted()
      )
      fixedRecord = addAboutToDocument(record)
    } writeOut(unixEpoch, ParsedResult(fixedRecord.id, fixedRecord.document))
  }

  private def getUnixEpoch: Long = System.currentTimeMillis() / 1000L

}

object LocalOaiHarvester {
  def main(args: Array[String]): Unit = {
    // -l http://oai.forum.jstor.org/oai/ -s 1041 -o artstor.xml -m oai_dc
    val spark = SparkSession.builder().appName("LocalOaiHarvester").master("local[6]").getOrCreate()
    val i3Conf = new Ingestion3Conf("conf/i3.conf", Some("artstor")).load()
    val harvester = new LocalOaiHarvester(spark, "artstor", i3Conf)
    val results = harvester.localHarvest()
    results.write.mode(SaveMode.Overwrite).format("json").save("artstor.jsonl")
    Files.delete(Path.of(harvester.tmpOutStr))
  }
}
