package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.DcmiTypeEnforcer.dcmiType
import dpla.ingestion3.mappers.rdf.DCMIType
import dpla.ingestion3.model.SkosConcept
import org.eclipse.rdf4j.model.IRI

import scala.collection.immutable.ListMap
import scala.io.Source

trait VocabEnforcer[T] {
  val enforceVocab: (T, Set[T]) => Boolean = (value, vocab) => {
    // TODO: Should this be case insensitive? It will fail on 'image' but pass on 'Image"
    vocab.contains(value)
  }

  val mapVocab: (T, Map[T, IRI]) => Option[IRI] = (value, vocab) => {
    // TODO string similarity comparision, this just does a strict lookup...
    vocab.get(value)
  }

  /**
    * Accepts a SkosConcept from the mapped record and attempts to lookup the mapped value
    * in an external vocabulary returning the preferred form.
    */
  val matchToSkosVocab: (SkosConcept, Map[String,String]) => SkosConcept = (originalValue, skosVocab) => {
    // convert to lower case
    // val lcOrigingalValue = originalValue.copy(providedLabel = Option(originalValue.providedLabel.getOrElse("").toLowerCase))
    val lcTerm = originalValue.providedLabel.getOrElse("").toLowerCase()
    val enrichedLexvo = skosVocab.get(lcTerm)

    // If the lookup performed above returned a match then we need to merge the providedLabel value
    // from originalValue with the enriched concept and scheme values. If no match was found these
    // step are essentially no-ops (setting None values).

    // For languages: The concept property should only contain ISO-639 term values. If not match is
    // found then it should remain blank and the original value remains in providedLabel.
    // FIXME Scheme not used
    // val schemeUri = Some(new URI("http://lexvo.org/id/iso639-3/"))

    SkosConcept(
      concept = enrichedLexvo,
      providedLabel = originalValue.providedLabel
    )
  }
}

object DcmiTypeEnforcer extends VocabEnforcer[String] {
  val dcmiType = DCMIType()

  val DcmiTypeStrings = Set(
    dcmiType.Collection,
    dcmiType.Dataset,
    dcmiType.Event,
    dcmiType.Image,
    dcmiType.InteractiveResource,
    dcmiType.MovingImage,
    dcmiType.PhysicalObject,
    dcmiType.Service,
    dcmiType.Software,
    dcmiType.Sound,
    dcmiType.StillImage,
    dcmiType.Text
  ).map(_.getLocalName)

  val enforceDcmiType: (String) => Boolean = enforceVocab(_, DcmiTypeStrings)
}

object DcmiTypeMapper extends VocabEnforcer[String] {
  val dcmiType = DCMIType()

  val DcmiTypeMap: Map[String, IRI] = Map(
    "appliance" -> dcmiType.Image,
    "audio" -> dcmiType.Sound,
    "book" -> dcmiType.Text,
    "cartographic" -> dcmiType.Image,
    "container" -> dcmiType.Image,
    "correspondence" -> dcmiType.Text,
    "costume" -> dcmiType.Image,
    "drawing" -> dcmiType.Image,
    "electronic component" -> dcmiType.Image,
    "electronic resource" -> dcmiType.InteractiveResource,
    "equipment" -> dcmiType.Image,
    "film" -> dcmiType.MovingImage,
    "finding aid" -> dcmiType.Collection,
    "frame" -> dcmiType.Image,
    "furnishing" -> dcmiType.Image,
    "furniture" -> dcmiType.Image,
    "illumination" -> dcmiType.Image,
    "image" -> dcmiType.Image,
    "jewelry" -> dcmiType.Image,
    "journal" -> dcmiType.Text,
    "magazine" -> dcmiType.Text,
    "manuscript" -> dcmiType.Text,
    "mixed material" -> dcmiType.Image,
    "motion picture" -> dcmiType.MovingImage,
    "moving image" -> dcmiType.MovingImage,
    "movingimage" -> dcmiType.MovingImage,
    "notated music" -> dcmiType.Image,
    "object" -> dcmiType.PhysicalObject,
    "online collection" -> dcmiType.Collection,
    "online exhibit" -> dcmiType.InteractiveResource,
    "online text" -> dcmiType.Text,
    "oral history recording" -> dcmiType.Sound,
    "painting" -> dcmiType.Image,
    "photograph" -> dcmiType.Image,
    "physicalobject" -> dcmiType.PhysicalObject,
    "postcard" -> dcmiType.Image,
    "poster" -> dcmiType.Image,
    "print" -> dcmiType.Image,
    "publication" -> dcmiType.Text,
    "sample book" -> dcmiType.Image,
    "sculpture" -> dcmiType.Image,
    "sound" -> dcmiType.Sound,
    "specimen" -> dcmiType.Image,
    "statue" -> dcmiType.Image,
    "stillimage" -> dcmiType.StillImage,
    "text" -> dcmiType.Text,
    "textile" -> dcmiType.Image,
    "tool" -> dcmiType.Image,
    "video game" -> dcmiType.InteractiveResource,
    "video" -> dcmiType.MovingImage,
    "writing" -> dcmiType.Text,
    "written" -> dcmiType.Text
  )

  val mapDcmiType: (String) => Option[IRI] = mapVocab(_, DcmiTypeMap)
}

object DcmiTypeStringMapper extends VocabEnforcer[String] {

  val mapDcmiTypeString: (IRI) => String = {
    case dcmiType.InteractiveResource => "InteractiveResource"
    case dcmiType.MovingImage => "Moving Image"
    case dcmiType.PhysicalObject => "Physical Object"
    case dcmiType.StillImage => "Still Image"
    case iri => iri.getLocalName
  }
}

/**
  * Reads in iso693-X data from CSV files and generates a
  * map to reconcile language abbreviations to the full term
  *
  */
object LanguageMapper extends VocabEnforcer[String] {

  private val languageFiles = Seq(
    // FIXME These should not be hard coded
    "/languages/iso639-2.csv",
    "/languages/iso639-3.csv",
    "/languages/dpla-lang.csv")

  // Reads a file
  private def readFile(path: String): Map[String, String] = {
    val stream = getClass.getResourceAsStream(path)
    Source.fromInputStream(stream)
      .getLines()
      .filterNot(l => l.startsWith("#")) // Ignore lines that start with #
      .map(line => line.split(",")) // Split line on comma
      .map(cols => cols(0) -> cols(1).replaceAll("\"", "")) // Remove double quotes from labels
      .toMap
  }

  // Create the language lookup map
  private val languageMap  =
    /**
      * Spark could be used with pattern matching to load CSV for TAB files in a dir
      * but that seems like overkill here. Probably the best way to go for now is supporting
      * passing a list of file paths to load and then we can inject those values via a config
      * or some other extensible way in the future. The Seq() can be hard coded for now.
      */
    languageFiles.flatMap(file => readFile(file)).toMap

  /**
    * Takes the SkosConcept value created during mapping and attempts find a prefLabel for that
    * term by looking up SkosConcept.providedLabel against iso639-x codes in languageMap. Returns
    * either the original SkosConcept value for a new SkosCocnept with the ISO label in SkosConcept.concept
    * and the original value in SkosConcept.providedLabel
    */
  val mapLanguage: (SkosConcept) => SkosConcept = matchToSkosVocab(_, languageMap)


  /**
    * Gets an ordered version of the language map
    * @return
    */
  def getLanuageMap = ListMap(languageMap.toSeq.sortBy(_._1):_*)
}
