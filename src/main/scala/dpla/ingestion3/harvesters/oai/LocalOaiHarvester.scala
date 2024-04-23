package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{Harvester, LocalHarvester}
import dpla.ingestion3.harvesters.oai.refactor.{
  AllRecordsOaiRelation,
  AllSetsOaiRelation,
  BlacklistOaiRelation,
  OaiConfiguration,
  OaiError,
  OaiProtocol,
  OaiRecord,
  OaiRelation,
  OaiSet,
  WhitelistOaiRelation
}
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class LocalOaiHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(spark, shortName, conf) {

  override def mimeType: String = "application_xml"

  override def localHarvest(): DataFrame = {

    val logger = LogManager.getLogger(this.getClass)

    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L

    val readerOptions: Map[String, String] = Map(
      "verb" -> conf.harvest.verb,
      "metadataPrefix" -> conf.harvest.metadataPrefix,
      "harvestAllSets" -> conf.harvest.harvestAllSets,
      "setlist" -> conf.harvest.setlist,
      "blacklist" -> conf.harvest.blacklist,
      "endpoint" -> conf.harvest.endpoint,
      "removeDeleted" -> Some("true")
    ).collect { case (key, Some(value)) => key -> value }

    val oaiConfig = OaiConfiguration(readerOptions)
    val oaiMethods = new OaiProtocol(oaiConfig)
    val avroWriter = getAvroWriter

    (oaiConfig.setlist, oaiConfig.harvestAllSets, oaiConfig.blacklist) match {
      case (None, false, Some(blacklist)) =>
        val originalSets = oaiMethods
          .listAllSetPages()
          .iterator
          .flatMap(oaiMethods.parsePageIntoSets)

        val nonBlacklistedSets = originalSets.filter {
          case Right(OaiSet(set, _)) => blacklist.contains(set)
          case _                     => true
        }

        for {
          set <- nonBlacklistedSets
          pageEither <- oaiMethods.listAllRecordPagesForSet(set)
          record <- oaiMethods.parsePageIntoRecords(
            pageEither,
            removeDeleted = oaiConfig.removeDeleted
          )
        } {
          record match {
            case Left(OaiError(message, url)) => logger.error(f"$message, $url")
            case Right(OaiRecord(id, document, setIds)) =>
              val genericRecord = new GenericData.Record(Harvester.schema)
              genericRecord.put("id", id)
              genericRecord.put("ingestDate", unixEpoch)
              genericRecord.put("provider", shortName)
              genericRecord.put("document", document)
              genericRecord.put("mimetype", mimeType)
              avroWriter.append(genericRecord)
          }
        }

      case (Some(setList), false, None) =>
      // new WhitelistOaiRelation(oaiConfig, oaiMethods)(sqlContext)
      case (None, false, None) =>
      // new AllRecordsOaiRelation(oaiConfig, oaiMethods)(sqlContext)
      case (None, true, None) =>
      // new AllSetsOaiRelation(oaiConfig, oaiMethods)(sqlContext)
      case _ =>
        throw OaiHarvesterException(
          "Unable to determine harvest type from parameters."
        )
    }

    avroWriter.flush()

    val df = spark.read.format("avro").load(tmpOutStr)
    df
  }
}
