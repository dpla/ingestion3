package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{LocalHarvester, ParsedResult}
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime, ZoneId}

class LocalOaiHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf,
    harvestLogger: OaiHarvestLogger = OaiHarvestLogger.Noop
) extends LocalHarvester(shortName, conf) {

  private val readerOptions: Map[String, String] = Map(
    "verb" -> conf.harvest.verb,
    "metadataPrefix" -> conf.harvest.metadataPrefix,
    "httpVersion" -> conf.harvest.httpVersion,
    "includeSetNames" -> conf.harvest.includeSetNames,
    "harvestAllSets" -> conf.harvest.harvestAllSets,
    "setlist" -> conf.harvest.setlist,
    "blacklist" -> conf.harvest.blacklist,
    "endpoint" -> conf.harvest.endpoint,
    "removeDeleted" -> Some("true"),
    "sleep" -> conf.harvest.sleep
  ).collect { case (key, Some(value)) => key -> value }

  private val oaiConfig = OaiConfiguration(readerOptions)
  private val oaiMethods = new OaiProtocol(oaiConfig, harvestLogger)

  override def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  // setSpec -> setName for every set the endpoint lists, fetched once per
  // harvest. Only used when harvest.includeSetNames is enabled.
  private lazy val setNames: Map[String, String] = {
    val names = LocalOaiHarvester.setNamesFromSets(
      oaiMethods
        .listAllSetPages()
        .iterator
        .flatMap(oaiMethods.parsePageIntoSets)
    )
    if (names.isEmpty)
      throw OaiHarvesterException(
        "harvest.includeSetNames is enabled but ListSets returned no set names."
      )
    names
  }

  override def harvest: DataFrame = {

    // Fetch set names up front so a ListSets failure aborts before any records
    // are written (and isn't reported as a record parse error).
    if (oaiConfig.includeSetNames) setNames

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

    close()
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

    try {
      val documentXml = scala.xml.XML.loadString(document)
      val header = documentXml \ "header"
      val metadata = documentXml \ "metadata"
      // Preserve any provider-supplied <about> content alongside the OAI request
      // bookkeeping, rather than discarding it. Some providers put meaningful data
      // here — e.g. JSTOR/Ithaka supplies the contributing institution as
      // <about><oai_dc:dc><oai_dc:publisher>. Only element children are carried
      // over (whitespace text nodes dropped).
      val providerAbout =
        (documentXml \ "about").flatMap(_.child).collect { case e: scala.xml.Elem =>
          e
        }
      val setNameElements =
        if (oaiConfig.includeSetNames)
          LocalOaiHarvester.setNameElements(header, setNames)
        else Seq.empty
      val about =
        <about>{requestElement}{resumptionToken}{responseDate}{setNameElements}{providerAbout}</about>
      val record = <record>{header}{metadata}{about}</record>
      oaiRecord.copy(document = record.toString)
    } catch {
      case e: Exception =>
        throw new OaiHarvestException(
          requestInfo = info,
          url = oaiConfig.endpoint,
          stage = "record_parse",
          firstId = Some(oaiRecord.id),
          lastId = Some(oaiRecord.id),
          cause = e
        )
    }
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

  /** setSpec -> setName from parsed ListSets entries (sets without a name are
    * skipped).
    */
  def setNamesFromSets(sets: Iterator[OaiSet]): Map[String, String] =
    sets.flatMap(set => set.name.map(set.id -> _)).toMap

  /** One <setName spec="..."> element per header setSpec that has a known name,
    * in header order.
    */
  def setNameElements(
      header: scala.xml.NodeSeq,
      names: Map[String, String]
  ): Seq[scala.xml.Elem] =
    (header \ "setSpec").map(_.text).flatMap { spec =>
      names.get(spec).map(name => <setName spec={spec}>{name}</setName>)
    }
}
