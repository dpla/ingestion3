package dpla.ingestion3.harvesters.oai.refactor

import java.io.{File, FileWriter}

import dpla.ingestion3.harvesters.HarvesterExceptions.{throwMissingArgException, throwUnrecognizedArgException, throwValidationException}
import dpla.ingestion3.harvesters.oai._
import dpla.ingestion3.utils.HttpUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.TraversableOnce

class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): OaiRelation = {

    val oaiProtocol: OaiProtocol = new OaiProtocol {

    }

    OaiConfiguration(parameters).getHarvestType match {
      case blackListHarvest: BlacklistHarvest => new BlacklistOaiRelation(blackListHarvest, oaiProtocol)(sqlContext)
      case whitelistHarvest: WhitelistHarvest => new WhitelistOaiRelation(whitelistHarvest, oaiProtocol)(sqlContext)
      case allRecordsHarvest: AllRecordsHarvest => new AllRecordsOaiRelation(allRecordsHarvest, oaiProtocol)(sqlContext)
      case allSetsHarvest: AllSetsHarvest => new AllSetsOaiRelation(allSetsHarvest, oaiProtocol)(sqlContext)
    }
  }
}

case class OaiConfiguration(parameters: Map[String, String]) {

  def endpoint: String = {
    val endpoint = parameters.get("endpoint")
    (endpoint, HttpUtils.validateUrl(endpoint.getOrElse(""))) match {
      // An endpoint url was provided and is reachable
      case (Some(url), true) => url
      // An endpoint url was provided but is unreachable
      case (Some(url), false) => throwValidationException(s"OAI endpoint $url is not reachable.")
      // No endpoint parameter was provided, it is a redundant validation of those in OaiHarvesterConf
      case (None, false) => throwMissingArgException("endpoint")
      // Something very unexpected
      case _ => throwValidationException(s"Unable to validate the OAI endpoint.")
    }
  }

  def verb: String = {
    val verb = parameters.get("verb")
    (verb, oaiVerbs.contains(verb.getOrElse(""))) match {
      // A verb parameter was given and it matches the list of valid OAI verbs
      case (Some(v), true) => v
      // A verb parameter was given but it is not a supported OAI verb
      case (Some(v), false) => throwValidationException(s"$v is not a valid or currently supported OAI verb")
      // A verb was not provided, it is a redundant validation of those in OaiHarvesterConf
      case (None, false) => throwMissingArgException("verb")
      // Something very unexpected
      case _ => throwValidationException("Unable to validate OAI verb.")
    }
  }

  def metadataPrefix: Option[String] = parameters.get("metadataPrefix")

  def harvestAllSets: Boolean = parameters.get("harvestAllSets")
    .map(_.toLowerCase) match {
    case Some("true") => true
    case Some("false") => false
    case None => false
    case x => throwUnrecognizedArgException(s"harvestAllSets => $x")
  }

  def setlist: Option[Array[String]] = parameters.get("setlist").map(parseSets)

  def blacklist: Option[Array[String]] = parameters.get("blacklist").map(parseSets)

  def getHarvestType: HarvestType = ???

  private [this] val oaiVerbs = Set("ListRecords", "ListSets")

  private[this] def parseSets(sets: String): Array[String] = sets.split(",").map(_.trim)

  // Ensure that set handlers are only used with the "ListRecords" verb.
  private[this] def validateArguments: Boolean =
    verb == "ListRecords" ||
      Seq(harvestAllSets, setlist, blacklist)
        .forall(x => x == None || x == false)

  require(
    validateArguments,
    "The following can only be used with the 'ListRecords' verb: harvestAllSets, setlist, blacklist"
  )
}

abstract sealed class HarvestType

case class BlacklistHarvest(endpoint: String, metadataPrefix: String, blacklist: Array[String]) extends HarvestType

case class WhitelistHarvest(endpoint: String, metadataPrefix: String, setlist: Array[String]) extends HarvestType

case class AllRecordsHarvest(endpoint: String, metadataPrefix: String) extends HarvestType

case class AllSetsHarvest(endpoint: String, metadataPrefix: String) extends HarvestType

abstract class OaiRelation extends BaseRelation with TableScan with Serializable {
  /**
    * Set the schema for the DataFrame that will be returned on load.
    * Columns:
    *   - set: OaiSet
    *   - record: OaiRecord
    *   - error: String
    * Each row will have a non-null value for ONE of sets, records, or error.
    */
  override def schema: StructType = {
    val setType = getStruct[OaiSet]
    val recordType = getStruct[OaiRecord]
    val errorType = getStruct[OaiError]
    val setField = StructField("set", setType, nullable = true)
    val recordField = StructField("record", recordType, nullable = true)
    val errorField = StructField("error", errorType, nullable = true)
    StructType(Seq(setField, recordField, errorField))
  }

  private def getStruct[T] =
    ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

  override def buildScan(): RDD[Row]
}

// Each of the following should start by creating a DataFrame or RDD and then chaining flatMap() or map() calls
// on it until it results in a RDD[Row] of result values per the schema in OaiRelation.

class AllRecordsOaiRelation(allRecordsHarvest: AllRecordsHarvest)
                           (@transient oaiProtocol: OaiProtocol)
                           (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  //list all the record pages, flat map to records
  override def buildScan(): RDD[Row] = {

    val tempFile = File.createTempFile("oai", ".txt")
    val writer = new FileWriter(tempFile)

    try {
      for (page <- oaiProtocol.listAllRecordPages)
        writer.write(page.replaceAll("\n", " "))
      //todo error handling

    } finally {
      IOUtils.closeQuietly(writer)
    }

    sqlContext.read.text(tempFile.getAbsolutePath)
      .flatMap(row => oaiProtocol.parsePageIntoRecords(row.getString(0)))
      .map(Row(None, _, None))
      .rdd
  }
}

class AllSetsOaiRelation(allSetsHarvest: AllSetsHarvest)
                        (@transient oaiProtocol: OaiProtocol)
                        (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  override def buildScan(): RDD[Row] = {
    sqlContext.sparkContext
      .parallelize[String](oaiProtocol.listAllSets.toSeq)
      .flatMap(set => oaiProtocol.listAllRecordPagesForSet(set))
      .flatMap(oaiProtocol.parsePageIntoRecords(_)) //TODO

  }
}

class BlacklistOaiRelation(blacklistHarvest: BlacklistHarvest)
                          (@transient oaiProtocol: OaiProtocol)
                          (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  override def buildScan(): RDD[Row] = ???
}

class WhitelistOaiRelation(whitelistHarvest: WhitelistHarvest)
                          (@transient oaiProtocol: OaiProtocol)
                          (@transient override val sqlContext: SQLContext)
  extends OaiRelation {
  override def buildScan(): RDD[Row] = ???
}

trait OaiProtocol {

  def listAllRecordPagesForSet(set: String): TraversableOnce[String]

  def parsePageIntoRecords(page: String): TraversableOnce[OaiResponse]

  def listAllSets: TraversableOnce[String]

  def listAllRecordPages: TraversableOnce[String]
}