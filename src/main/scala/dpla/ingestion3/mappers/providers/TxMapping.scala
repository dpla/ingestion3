package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, EdmTimeSpan, EdmWebResource, URI, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JsonAST
import org.json4s.JsonDSL._

import scala.collection.mutable.ArrayBuffer
import scala.xml.NodeSeq

class TxMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {
  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  override def getProviderName: String = "texas"

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    val dataProviders = extractStrings(data \ "header" \ "setSpec")
      .map(setSpec => TxMapping.dataproviderTermLabel.getOrElse(setSpec.split(":").last, ""))
      .filter(_.nonEmpty)

    Seq(nameOnlyAgent(dataProviders.last))
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (metadata(data) \ "identifier")
      .filter(node => filterAttribute(node, "qualifier", "itemURL"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (metadata(data) \ "identifier")
      .filter(node => filterAttribute(node, "qualifier", "thumbnailURL"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("The Portal to Texas History"),
      uri = Some(URI("http://dp.la/api/contributor/the_portal_to_texas_history"))
    )

  override def sidecar(data: Document[NodeSeq]): JsonAST.JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  override def useProviderName: Boolean = true

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")
      .map(_.trim)

  // dpla.sourceResource
  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractName(metadata(data), "contributor")

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractName(metadata(data), "creator")

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = {
    val creationDates = (data \ "metadata" \ "date")
      .filter(node => filterAttribute(node, "qualifier", "creation"))
      .flatMap(extractStrings)
      .map(stringOnlyTimeSpan)
      .headOption

    val otherDates = (data \ "metadata" \ "date")
      .filterNot(node => filterAttribute(node, "qualifier", "digitized") | filterAttribute(node, "qualifier", "embargoUntil"))
      .flatMap(extractStrings)
      .map(stringOnlyTimeSpan)
      .headOption

    // Return only the first instance of either creation date or any other valid date
    (creationDates ++ otherDates).headOption.toSeq
  }

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadata(data) \ "description")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadata(data) \ "resourceType")
      .map(_.splitAtDelimiter("_").head)
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadata(data) \ "identifier")

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(metadata(data) \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = {
    val qualifiers = Seq("placeName", "placePoint", "placeBox")

    (metadata(data) \ "coverage")
      .filter(node => filterAttributeListOptions(node, "qualifier", qualifiers))
      .flatMap(extractStrings)
      .map(nameOnlyPlace)
  }

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    // Only create a publisher value when <publisher> containers both <location> and <name> sub-properties
    //    <untl:publisher>
    //      <untl:location>Philadelphia</untl:location>
    //      <untl:name>Charles Desilver</untl:name>
    //    </untl:publisher>

    //    For the above example, the expected mapped publisher value is: 'Philadelphia: Charles Desilver'

    val locations = extractStrings(metadata(data) \ "publisher" \ "location")
    val names = extractStrings(metadata(data) \ "publisher" \ "name")

    locations.zipAll(names, None, None).flatMap {
      case (location: String, name: String) => Some(s"$location: $name")
      case (_, _) => None
    }.map(nameOnlyAgent)
  }

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractStrings(data \ "metadata" \ "relation").map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(metadata(data) \ "rights")
      .map(text => TxMapping.rightsTermLabel.getOrElse(text, text))

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(metadata(data) \ "subject")
      .map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(metadata(data) \ "title")

  override def `type`(data: Document[NodeSeq]): AtLeastOne[String] = {
    // This greatly simplifies the ingestion1 mapping and pushes the filtering logic from ingestion1 to the ingestion3
    // type enrichment
    extractStrings(metadata(data) \ "format")
  }

  /**
    * Helper method to extract value directly associated with property or <name> sub-property
    *
    * @param data
    * @param property
    * @return
    */
  def extractName(data: NodeSeq, property: String): ZeroToMany[EdmAgent] = {
    (data \ property)
      .flatMap(node => {
        val name = extractStrings(node \ "name")
        val propertyValue = node.child match {
          case _: ArrayBuffer[String] => Seq(node.text)
          case _ => Seq()
        }

        // If propertyValue is empty then return the value of the <name> sub-property
        if(propertyValue.isEmpty)
          name
        else
          propertyValue
      })
      .map(nameOnlyAgent)
  }

  /**
    * Helper method to get to metadata root
    *
    * @param data
    * @return
    */
  def metadata(data: NodeSeq): NodeSeq = data \ "metadata" \ "metadata"
}


object TxMapping {
  val rightsTermLabel: Map[String, String] = Map[String, String](
    "by" -> "License: Attribution.",
    "by-nc"-> "License: Attribution Noncommercial.",
    "by-nc-nd"-> "License: Attribution Non-commercial No Derivatives.",
    "by-nc-sa"-> "License: Attribution Noncommercial Share Alike.",
    "by-nd"-> "License: Attribution No Derivatives.",
    "by-sa"-> "License: Attribution Share Alike.",
    "copyright"-> "License: Copyright.",
    "pd"-> "License: Public Domain."
  )

  val dataproviderTermLabel = Map[String, String](
    "ABB" -> "Bryan Wildenthal Memorial Library (Archives of the Big Bend)",
    "ABPL" -> "Abilene Public Library",
    "ACGS" -> "Anderson County Genealogical Society",
    "ACHC" -> "Anderson County Historical Commission",
    "ACRM" -> "Amon Carter Museum",
    "ACTUMT" -> "Archives of the Central Texas Conference United Methodist Church",
    "ACUL" -> "Abilene Christian University Library",
    "APL" -> "Alvord Public Library",
    "ASPL" -> "Austin History Center, Austin Public Library",
    "ASTC" -> "Austin College",
    "BCHC" -> "Bosque County Historical Commission",
    "BDPL" -> "Boyce Ditto Public Library",
    "BPL" -> "Lena Armstrong Public Library",
    "CCGS" -> "Collin County Genealogical Society",
    "CCHM" -> "North Texas History Center",
    "CCHS" -> "Clay County Historical Society",
    "CCMH" -> "Childress County Heritage Museum",
    "CCMS" -> "Corpus Christi Museum of Science and History",
    "CHM" -> "Clark Hotel Museum",
    "CHOS" -> "Courthouse-on-the-Square Museum",
    "CTRM" -> "Cattle Raisers Museum",
    "CUA" -> "Concordia University at Austin",
    "CWTC" -> "Cowtown Coliseum",
    "DAPL" -> "Dallas Public Library",
    "DHS" -> "Dallas Historical Society",
    "DHVG" -> "Dallas Heritage Village",
    "DPL" -> "Denton Public Library",
    "DSCL" -> "Deaf Smith County Library",
    "EPL" -> "Euless Public Library",
    "ETGS" -> "East Texas Genealogical Society",
    "FBCM" -> "FBC Heritage Museum",
    "FBM" -> "Fort Bend Museum",
    "FCPA" -> "First Christian Church of Port Arthur",
    "FPL" -> "Ferris Public Library",
    "FWJA" -> "Fort Worth Jewish Archives",
    "FWPL" -> "Fort Worth Public Library",
    "GLO" -> "Texas General Land Office",
    "GMHP" -> "Genevieve Miller Hitchcock Public Library",
    "GR" -> "George Ranch Historical Park",
    "HCGS" -> "Hutchinson County Genealogical Society",
    "HHSM" -> "Heritage House Museum",
    "HPUL" -> "Howard Payne University Library",
    "HSUL" -> "Hardin-Simmons University Library",
    "IPL" -> "Irving Archives",
    "JFRM" -> "Jacob Fontaine Religious Museum",
    "KCT" -> "Killeen City Library System",
    "KHSY" -> "Kemah Historical Society",
    "KUMC" -> "Krum United Methodist Church",
    "LCVG" -> "Log Cabin Village",
    "LDMA" -> "Lockheed Martin Aeronautics Company, Fort Worth",
    "LPL" -> "Laredo Public Library",
    "MARD" -> "Museum of the American Railroad",
    "MFAH" -> "The Museum of Fine Arts, Houston",
    "MMMM" -> "Medicine Mound Museum",
    "MMPL" -> "Moore Memorial Public Library",
    "MMUL" -> "McMurry University Library",
    "MPLI" -> "Marshall Public Library",
    "MRPL" -> "Marfa Public Library",
    "OKHS" -> "Oklahoma Historical Society",
    "OSAGC" -> "Old Settler's Association of Grayson County",
    "PANAM" -> "The University of Texas-Pan American",
    "PCBG" -> "Private Collection of Bouncer Goin",
    "PCCRD" -> "Private Collection of Charles R. Delphenis",
    "PCCRS" -> "Private Collection of Caroline R. Scrivner Richards",
    "PCHBM" -> "Private Collection of Howard and Brenda McClurkin",
    "PCJEH" -> "Private Collection of Joe E. Haynes",
    "PPL" -> "Palestine Public Library",
    "RPL" -> "Richardson Public Library",
    "RSMT" -> "Rose Marine Theatre",
    "SJMH" -> "San Jacinto Museum of History",
    "SMU" -> "Southern Methodist University Libraries",
    "SRPL" -> "Sanger Public Library",
    "STAR" -> "Star of the Republic Museum",
    "SSPL" -> "Sulphur Springs Public Library",
    "TCC" -> "Tarrant County College NE, Heritage Room",
    "TCU" -> "Texas Christian University",
    "TSGS" -> "Texas State Genealogical Society",
    "TSLAC" -> "Texas State Library and Archives Commission",
    "TWU" -> "Texas Woman's University",
    "TXLU" -> "Texas Lutheran University",
    "UH" -> "University of Houston Libraries' Special Collections",
    "UNT" -> "UNT Libraries",
    "UT" -> "University of Texas",
    "UTA" -> "University of Texas at Arlington Library",
    "UTSW" -> "UT Southwestern Medical Center Library",
    "VCHC" -> "Val Verde County Historical Commission",
    "WCHM" -> "Wolf Creek Heritage Museum",
    "WEAC" -> "Weatherford College",
    "WEBM" -> "Weslaco Museum",
    "OTHER" -> "Other",
    "RICE" -> "Rice University Woodson Research Center",
    "HCDC" -> "Henderson County District Clerk's Office",
    "ORMM" -> "The Old Red Museum",
    "SFMDP" -> "The Sixth Floor Museum at Dealey Plaza",
    "CCTX" -> "City of Clarendon",
    "PCEBF" -> "The Private Collection of the Ellis and Blanton Families",
    "DCCCD" -> "Dallas County Community College District",
    "THF" -> "Texas Historical Foundation",
    "SWCL" -> "Swisher County Library",
    "WYU" -> "Wiley College",
    "LBJSM" -> "LBJ Museum of San Marcos",
    "DSMA" -> "Dallas Municipal Archives",
    "FRLM" -> "French Legation Museum ",
    "PCSF" -> "The Private Collection of the Sutherlin Family",
    "ARPL" -> "Arlington Public Library and Fielder House",
    "BEHC" -> "Bee County Historical Commission",
    "CGHPC" -> "City of Granbury Historic Preservation Commission",
    "ELPL" -> "El Paso Public Library ",
    "GPHO" -> "Grand Prairie Historical Organization",
    "MLCC" -> "Matthews Family and Lambshead Ranch",
    "NELC" -> "Northeast Lakeview College",
    "PAPL" -> "Port Arthur Public Library",
    "PBPM" -> "Permian Basin Petroleum Museum, Library and Hall of Fame",
    "RVPM" -> "River Valley Pioneer Museum",
    "SFASF" -> "Stephen F. Austin Assn. dba Friends of the San Felipe State Historic Site",
    "UTSA" -> "University of Texas at San Antonio",
    "VCUH" -> "Victoria College/University of Houston-Victoria Library",
    "HCLY" -> "Hemphill County Library",
    "BACHS" -> "Bartlett Activities Center and the Historical Society of Bartlett",
    "CAH" -> "The Dolph Briscoe Center for American History ",
    "UNTA" -> "UNT Archives",
    "UNTRB" -> "UNT Libraries Rare Book and Texana Collections",
    "UNTCVA" -> "UNT College of Visual Arts + Design",
    "UNTDP" -> "UNT Libraries Digital Projects Unit",
    "UNTGD" -> "UNT Libraries Government Documents Department",
    "UNTML" -> "UNT Music Library",
    "UNTP" -> "UNT Press",
    "UNTLML" -> "UNT Media Library",
    "UNTCOI" -> "UNT College of Information",
    "BRPL" -> "Breckenridge Public Library",
    "STWCL" -> "Stonewall County Library",
    "NPSL" -> "Nicholas P. Sims Library",
    "PCJB" -> "Private Collection of Jim Bell",
    "MQPL" -> "Mesquite Public Library",
    "BWPL" -> "Bell/Whittington Public Library",
    "CHMH" -> "Cedar Hill Museum of History",
    "CLHS" -> "Cleveland Historic Society",
    "CKCL" -> "Cooke County Library",
    "DFFM" -> "Dallas Firefighters Museum",
    "FSML" -> "Friench Simpson Memorial Library",
    "HSCA" -> "Harris County Archives",
    "HTPL" -> "Haslet Public Library",
    "LVPL" -> "Longview Public Library",
    "MWSU" -> "Midwestern State University",
    "STPC" -> "St. Philips College",
    "UTHSC" -> "University of Texas Health Science Center Libraries",
    "WCHS" -> "Wilson County Historical Society",
    "TSHA" -> "Texas State Historical Association",
    "MCMPL" -> "McAllen Public Library",
    "UNTLTC" -> "UNT Linguistics and Technical Communication Department",
    "PCMB" -> "Private Collection of Melvin E. Brewer",
    "SGML" -> "Singletary Memorial Library",
    "URCM" -> "University Relations, Communications & Marketing department for UNT",
    "TXDTR" -> "Texas Department of Transportation",
    "TYPL" -> "Taylor Public Library",
    "WILLM" -> "The Williamson Museum",
    "ATPS" -> "Austin Presbyterian Theological Seminary",
    "BUCHC" -> "Burnet County Historical Commission",
    "DHPS" -> "Danish Heritage Preservation Society",
    "GCHS" -> "Gillespie County Historical Society",
    "HMRC" -> "Houston Metropolitan Research Center at Houston Public Library",
    "ITC" -> "University of Texas at San Antonio Libraries Special Collections",
    "DISCO" -> "Digital Scholarship Cooperative (DiSCo)",
    "MAMU" -> "Mexic-Arte Museum",
    "MMLUT" -> "Moody Medical Library, UT",
    "MGC" -> "Museum of the Gulf Coast",
    "NML" -> "Nesbitt Memorial Library",
    "PAC" -> "Panola College ",
    "PJFC" -> "Price Johnson Family Collection",
    "SAPL" -> "San Antonio Public Library",
    "AMSC" -> "Anne and Mike Stewart Collection",
    "TSU" -> "Tarleton State University",
    "STPRB" -> "Texas State Preservation Board",
    "UNTCAS" -> "UNT College of Arts and Sciences",
    "UNTCOE" -> "UNT College of Engineering",
    "UNTCPA" -> "UNT College of Public Affairs and Community Service",
    "STXCL" -> "South Texas College of Law",
    "CPL" -> "Carrollton Public Library",
    "CWCM" -> "Collingsworth County Museum",
    "PCMC" -> "Private Collection of Mike Cochran",
    "NMPW" -> "National Museum of the Pacific War/Admiral Nimitz Foundation",
    "SRH" -> "Sam Rayburn House Museum",
    "TCFA" -> "Talkington Clement Family Archives",
    "WTM" -> "Witte Museum",
    "UNTCED" -> "UNT College of Education",
    "BECA" -> "Beth-El Congregation Archives",
    "UNTCEDR" -> "UNT Center for Economic Development and Research",
    "DMA" -> "Dallas Museum of Art",
    "UTMDAC" -> "University of Texas MD Anderson Center",
    "UNTSMHM" -> "UNT College of Merchandising, Hospitality and Tourism",
    "UTEP" -> "University of Texas at El Paso",
    "UNTHSC" -> "UNT Health Science Center",
    "PPHM" -> "Panhandle-Plains Historical Museum",
    "AMPL" -> "Amarillo Public Library",
    "FWHC" -> "The History Center",
    "EFNHM" -> "Elm Fork Natural Heritage Museum",
    "UNTOHP" -> "UNT Oral History Program",
    "UNTCOB" -> "UNT College of Business ",
    "HCLB" -> "Hutchinson County Library, Borger Branch",
    "HPWML" -> "Harrie P. Woodson Memorial Library",
    "CTLS" -> "Central Texas Library System",
    "ARMCM" -> "Armstrong County Museum",
    "CHRK" -> "Cherokeean Herald",
    "DGS" -> "Dallas Genealogical Society",
    "UNTCOM" -> "UNT College of Music ",
    "MBIGB" -> "Museum of the Big Bend",
    "SCPL" -> "Schulenburg Public Library",
    "UNTCEP" -> "UNT Center For Environmental Philosophy",
    "UDAL" -> "University of Dallas",
    "PVAMU" -> "Prairie View A&M University ",
    "TWSU" -> "Texas Wesleyan University",
    "RGPL" -> "Rio Grande City Public Library",
    "UNTIAS" -> "UNT Institute of Applied Sciences",
    "UNTGSJ" -> "UNT Frank W. and Sue Mayborn School of Journalism",
    "BSTPL" -> "Bastrop Public Library",
    "SHML" -> "Stella Hill Memorial Library",
    "CAL" -> "Canyon Area Library",
    "MWHA" -> "Mineral Wells Heritage Association",
    "TAEA" -> "Texas Art Education Association",
    "EPCHS" -> "El Paso County Historical Society ",
    "CPPL" -> "Cross Plains Public Library",
    "LCHHL" -> "League City Helen Hall Library",
    "NCWM" -> "National Cowboy and Western Heritage Museum",
    "SWATER" -> "Sweetwater/Nolan County City-County Library",
    "UNTHON" -> "UNT Honors College",
    "PCJW" -> "Private Collection of Judy Wood and Jim Atkinson",
    "CRPL" -> "Crosby County Public Library",
    "DPKR" -> "City of Denton Parks and Recreation",
    "THC" -> "Texas Historical Commission",
    "BSAM" -> "Boy Scouts of America National Scouting Museum",
    "PCCW" -> "Private Collection of Carolyn West",
    "OCHS" -> "Orange County Historical Society",
    "DISD" -> "Denton Independent School District",
    "MINML" -> "Mineola Memorial Library",
    "CASML" -> "Casey Memorial Library",
    "UNTD" -> "UNT Dallas",
    "PTBW" -> "Private Collection of T. Bradford Willis",
    "UNTG" -> "University of North Texas Galleries",
    "SCHU" -> "Schreiner University",
    "TYHL" -> "Tyrrell Historical Library ",
    "TCAF" -> "Texas Chapter of the American Fisheries Society",
    "GIBBS" -> "Gibbs Memorial Library",
    "ATLANT" -> "Atlanta Public Library",
    "CCS" -> "City of College Station",
    "GCFV" -> "Grayson County Frontier Village",
    "PCTF" -> "Private Collection of the Tarver Family",
    "TAMS" -> "Texas Academy of Mathematics and Science",
    "CCHC" -> "Cherokee County Historical Commission",
    "PCBARTH" -> "Private Collection of Marie Bartholomew",
    "CCPL" -> "Corpus Christi Public Library",
    "UNTDCL" -> "UNT Dallas College of Law",
    "LAMAR" -> "Lamar State College - Orange",
    "SDEC" -> "St. David's Episcopal Church",
    "TDCD" -> "Travis County District Clerk's Office"
  )
}