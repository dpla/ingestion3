package dpla.ingestion3.mappers.utils

import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import org.apache.commons.codec.digest.DigestUtils
import org.json4s.JsonAST.JValue

import scala.xml.NodeSeq

trait Mapping[T] {

  implicit def unwrap(document: Document[T]): T = document.get

  // OreAggregation
  def dplaUri(data: Document[T]): ZeroToOne[URI] = None
  def dataProvider(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def originalRecord(data: Document[T]): ExactlyOne[String] = ""
  def hasView(data: Document[T]): ZeroToMany[EdmWebResource] = Seq()
  def intermediateProvider(data: Document[T]): ZeroToOne[EdmAgent] = None

  def isShownAt(data: Document[T]): ZeroToMany[EdmWebResource] = Seq()
  def `object`(data: Document[T]): ZeroToMany[EdmWebResource] = Seq() // full size image
  def preview(data: Document[T]): ZeroToMany[EdmWebResource] = Seq() // thumbnail

  def provider(data: Document[T]): ExactlyOne[EdmAgent] = emptyEdmAgent
  def edmRights(data: Document[T]): ZeroToMany[URI] = Seq()
  def sidecar(data: Document[T]): JValue = JValue()
  def tags(data: Document[T]): ZeroToMany[URI] = Seq()

  // SourceResource
  def alternateTitle(data: Document[T]): ZeroToMany[String] = Seq()
  def collection(data: Document[T]): ZeroToMany[DcmiTypeCollection] = Seq()
  def contributor(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def creator(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def date(data: Document[T]): ZeroToMany[EdmTimeSpan] = Seq()
  def description(data: Document[T]): ZeroToMany[String] = Seq()
  def extent(data: Document[T]): ZeroToMany[String] = Seq()
  def format(data: Document[T]): ZeroToMany[String] = Seq()
  def genre(data: Document[T]): ZeroToMany[SkosConcept] = Seq()
  def identifier(data: Document[T]): ZeroToMany[String] = Seq()
  def language(data: Document[T]): ZeroToMany[SkosConcept] = Seq()
  def place(data: Document[T]): ZeroToMany[DplaPlace] = Seq()
  def publisher(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def relation(data: Document[T]): ZeroToMany[LiteralOrUri] = Seq()
  def replacedBy(data: Document[T]): ZeroToMany[String] = Seq()
  def replaces(data: Document[T]): ZeroToMany[String] = Seq()
  def rights(data: Document[T]): AtLeastOne[String] = Seq()
  def rightsHolder(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def subject(data: Document[T]): ZeroToMany[SkosConcept] = Seq()
  def temporal(data: Document[T]): ZeroToMany[EdmTimeSpan] = Seq()
  def title(data: Document[T]): AtLeastOne[String] = Seq()
  def `type`(data: Document[T]): ZeroToMany[String] = Seq()


  /**
    Define the defaults for required field validations
  */
  val enforceDataProvider: Boolean  = true
  val enforceDplaUri: Boolean       = true
  val enforceEdmRights: Boolean     = false // Do not currently enforce the edmRights validation. WARN only
  val enforceIsShownAt: Boolean     = true
  val enforceObject: Boolean        = false // Do not enforce. Warn only if more than one object URL provided in source
  val enforceOriginalId: Boolean    = true
  val enforcePreview: Boolean       = false // Do not enforce. Warn only if more than one preview URL provided in source
  val enforceRights: Boolean        = true
  val enforceTitle: Boolean         = true

  /**
    Define the defaults validating for optional fields
  */
  val enforceCreator: Boolean       = true
  val enforceDate: Boolean          = true
  val enforceDescription: Boolean   = true
  val enforceFormat: Boolean        = true
  val enforceLanguage: Boolean      = true
  val enforcePlace: Boolean         = true
  val enforcePublisher: Boolean     = true
  val enforceSubject: Boolean       = true
  val enforceType: Boolean          = true

  // Base item uri
  private val baseDplaItemUri = "http://dp.la/api/items/"

  /**
    * Does the provider use a prefix (typically their provider shortname/abbreviation) to
    * salt the base identifier?
    *
    * @return Boolean
    */
  def useProviderName: Boolean

  /**
    * Extract the record's "persistent" identifier. Implementations should raise
    * an Exception if no ID can be extracted
    *
    * @return String Record identifier
    * @throws Exception If ID can not be extracted
    */
  def originalId(implicit data: Document[T]): ZeroToOne[String]

  /**
    * The provider's shortname abbreviation which is the value used to salt the
    * local identifier with when minting the DPLA identifier. Not all providers
    * use this "shortname--id" ID construction.
    *
    * @see buildProviderBaseId
    * @return String Provider shortname
    */
  def getProviderName: String = ""

  /**
    * Builds the ID to be hashed by either concatenating the provider's
    * abbreviated name and the persistent identifier for the record with
    * a double dash `--` or only the persistent identifier.
    *
    * Some providers use a name prefix and some do not. Please see Hub
    * mapping documentation and individual Extractor classes.
    *
    * ############################################################
    * # WARNING DO NOT CHANGE UNLESS YOU KNOW WHAT YOU ARE DOING #
    * ############################################################
    *
    * @return Option[String]
    */
  protected def buildProviderBaseId()(implicit data: Document[T]): Option[String] = {

    originalId match {
      case Some(id) =>
        if (useProviderName) Some(s"$getProviderName--$id")
        else Some(id)
      case None => None
    }
  }

  /**
    * Hashes the base ID
    *
    * ############################################################
    * # WARNING DO NOT CHANGE UNLESS YOU KNOW WHAT YOU ARE DOING #
    * ############################################################
    *
    * @return Option[String] MD5 hash of the base ID
    */
  protected def mintDplaId(implicit data: Document[T]): Option[String] =
    buildProviderBaseId.map(DigestUtils.md5Hex)

  /**
    * Builds the item URI
    *
    * @return Option[URI]
    */
  protected def mintDplaItemUri(implicit data: Document[T]): Option[URI] =
    mintDplaId.map(id => URI(s"$baseDplaItemUri$id"))
}

trait XmlMapping extends Mapping[NodeSeq]

trait JsonMapping extends Mapping[JValue]