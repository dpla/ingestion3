package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.oai.refactor.{
  OaiConfiguration,
  OaiProtocol,
  OaiRecord,
  OaiSet
}
import dpla.ingestion3.harvesters.{Harvester, LocalHarvester}
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class LocalOaiHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(spark, shortName, conf) {

  private val logger = LogManager.getLogger(this.getClass)

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

    val df = spark.read.format("avro").load(tmpOutStr)
    df
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
    } writeRecord(unixEpoch, record)
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
    } writeRecord(unixEpoch, record)
  }

  private def allRecordsHarvest(): Unit = {
    val unixEpoch = getUnixEpoch
    for {
      pageEither <- oaiMethods.listAllRecordPages()
      record <- oaiMethods.parsePageIntoRecords(
        pageEither,
        removeDeleted = oaiConfig.removeDeleted()
      )
    } writeRecord(unixEpoch, record)
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
    } writeRecord(unixEpoch, record)
  }

  private def writeRecord(
      unixEpoch: Long,
      record: OaiRecord
  ): Unit = {
    val genericRecord = new GenericData.Record(Harvester.schema)
    genericRecord.put("id", record.id)
    genericRecord.put("ingestDate", unixEpoch)
    genericRecord.put("provider", shortName)
    genericRecord.put("document", record.document)
    genericRecord.put("mimetype", mimeType)
    avroWriter.append(genericRecord)

  }

  private def getUnixEpoch = System.currentTimeMillis() / 1000L

}
