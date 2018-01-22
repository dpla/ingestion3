package dpla.ingestion3.enrichments

import java.net.URI

import dpla.ingestion3.enrichments.DcmiTypeEnforcer.dcmiType
import org.eclipse.rdf4j.model.IRI
import dpla.ingestion3.mappers.rdf.DCMIType
import dpla.ingestion3.model.SkosConcept

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
  val matchToSkosVocab: (SkosConcept, Map[SkosConcept,SkosConcept]) => SkosConcept = (originalValue, skosVocab) => {
    val enrichedLexvo = skosVocab.getOrElse(originalValue, originalValue)

    // TODO: Are these the correct mappings?
    // TODO: should properties like exactMatch, closeMatch and note be set? If so with what?
    // If the lookup performed above returned a match then we need to merge the providedLabel value
    // from originalValue with the enriched concept and scheme values. If no match was found these
    // step are essentially no-ops (setting None values).

    // For languages: The concept property should only contain ISO-639 term values. If not match is
    // found then it should remain blank and the original value remains in providedLabel.
    SkosConcept(
      concept = enrichedLexvo.concept,
      scheme = enrichedLexvo.scheme,
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
    "image" -> dcmiType.Image,
    "photograph" -> dcmiType.Image,
    "sample book" -> dcmiType.Image,
    "specimen" -> dcmiType.Image,
    "textile" -> dcmiType.Image,
    "frame" -> dcmiType.Image,
    "costume" -> dcmiType.Image,
    "statue" -> dcmiType.Image,
    "sculpture" -> dcmiType.Image,
    "container" -> dcmiType.Image,
    "jewelry" -> dcmiType.Image,
    "furnishing" -> dcmiType.Image,
    "furniture" -> dcmiType.Image,
    "drawing" -> dcmiType.Image,
    "print" -> dcmiType.Image,
    "painting" -> dcmiType.Image,
    "illumination" -> dcmiType.Image,
    "poster" -> dcmiType.Image,
    "appliance" -> dcmiType.Image,
    "tool" -> dcmiType.Image,
    "electronic component" -> dcmiType.Image,
    "postcard" -> dcmiType.Image,
    "equipment" -> dcmiType.Image,
    "cartographic" -> dcmiType.Image,
    "notated music" -> dcmiType.Image,
    "mixed material" -> dcmiType.Image,
    "text" -> dcmiType.Text,
    "book" -> dcmiType.Text,
    "publication" -> dcmiType.Text,
    "magazine" -> dcmiType.Text,
    "journal" -> dcmiType.Text,
    "correspondence" -> dcmiType.Text,
    "writing" -> dcmiType.Text,
    "written" -> dcmiType.Text,
    "manuscript" -> dcmiType.Text,
    "online text" -> dcmiType.Text,
    "audio" -> dcmiType.Sound,
    "sound" -> dcmiType.Sound,
    "oral history recording" -> dcmiType.Sound,
    "finding aid" -> dcmiType.Collection,
    "online collection" -> dcmiType.Collection,
    "electronic resource" -> dcmiType.InteractiveResource,
    "video game" -> dcmiType.InteractiveResource,
    "online exhibit" -> dcmiType.InteractiveResource,
    "moving image" -> dcmiType.MovingImage,
    "movingimage" -> dcmiType.MovingImage, // TODO: this looks like a typo...
    "motion picture" -> dcmiType.MovingImage,
    "film" -> dcmiType.MovingImage,
    "video" -> dcmiType.MovingImage,
    "object" -> dcmiType.PhysicalObject
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
  * Reads in ISO-693-3 data from tab-delimited file and generates a
  * map to reconcile language abbreviations to the full term
  *
  */
object LanguageMapper extends VocabEnforcer[String] {
  // Reads a file
  val readFile:(String) => Iterator[String] = (path) => {
    val stream = getClass.getResourceAsStream(path)
    Source.fromInputStream(stream).getLines
  }

  // Create the language lookup map
  val iso639Map = {
    // TODO: Make the path to the ISO data file configurable
    readFile("/iso-639-3.tab")
      // TODO: Find a clearer way to write the parsing of that file
      .map(_.split("\t"))
      .map(f = f => {
        // TODO: Does additional data need to be read in to more completely instantiate these objects?
        val languageAbbv = Option(f(0))
        val languageTerm = Option(f(1))
        val schemeUri = Option(new URI("http://lexvo.org/id/iso639-3/"))
        // Create a tuple >> (SkosConcept(abbv), SkosConcept(term, scheme)) that will be used to perform
        // lookups when enriching a value
        (SkosConcept(providedLabel = languageAbbv), SkosConcept(concept = languageTerm, scheme = schemeUri))
      })
      .toMap
  }
  // Attempt to enrich the original record value
  val mapLanguage: (SkosConcept) => SkosConcept = matchToSkosVocab(_, iso639Map)
}
