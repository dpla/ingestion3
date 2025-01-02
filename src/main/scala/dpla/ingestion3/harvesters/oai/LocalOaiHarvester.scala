package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.ParsedResult
import dpla.ingestion3.harvesters.oai.refactor.{OaiConfiguration, OaiProtocol, OaiRecord, OaiSet}
import dpla.ingestion3.harvesters.{Harvester, LocalHarvester}
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.{DataFrame, SparkSession}

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

  private def allowListHarvest(sets: Array[String]): Unit = {
    val unixEpoch = getUnixEpoch
    for {
      set <- sets
      page <- oaiMethods.listAllRecordPagesForSet(OaiSet(set, ""))
      record <- oaiMethods.parsePageIntoRecords(
        page,
        removeDeleted = oaiConfig.removeDeleted()
      )
    } writeOut(unixEpoch, ParsedResult(record.id, record.document))
  }

  private def allSetsHarvest(): Unit = {
    val unixEpoch = getUnixEpoch
    for {
      pageEither <- oaiMethods.listAllSetPages()
      set <- oaiMethods.parsePageIntoSets(pageEither)
      pageEither <- oaiMethods.listAllRecordPagesForSet(set)
      record <- oaiMethods.parsePageIntoRecords(
        pageEither,
        removeDeleted = oaiConfig.removeDeleted()
      )
    } writeOut(unixEpoch, ParsedResult(record.id, record.document))
  }

  private def allRecordsHarvest(): Unit = {
    val unixEpoch = getUnixEpoch
    for {
      pageEither <- oaiMethods.listAllRecordPages()
      record <- oaiMethods.parsePageIntoRecords(
        pageEither,
        removeDeleted = oaiConfig.removeDeleted()
      )
    } writeOut(unixEpoch, ParsedResult(record.id, record.document))
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
      pageEither <- oaiMethods.listAllRecordPagesForSet(set)
      record <- oaiMethods.parsePageIntoRecords(
        pageEither,
        removeDeleted = oaiConfig.removeDeleted()
      )
    } writeOut(unixEpoch, ParsedResult(record.id, record.document))
  }
  private def getUnixEpoch: Long = System.currentTimeMillis() / 1000L

}
