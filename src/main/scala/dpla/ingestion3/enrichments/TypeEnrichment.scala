package dpla.ingestion3.enrichments
import dpla.ingestion3.mappers.rdf.DCMIType
import org.eclipse.rdf4j.model.IRI

class TypeEnrichment extends VocabEnrichment[String] {

  // Performs term lookup
  private val typeLookup = new VocabLookup[String](
    (term: String) => normalizationFunc(term)
  )
  // Normalizes retrieval and accessed terms
  private def normalizationFunc(term: String): String = term.toLowerCase.trim

  // DCMIType mappings
  val dcmiType = DCMIType()

  val DcmiTypeMap: Map[String, IRI] = Map(
    "administrative record" -> dcmiType.Text,
    "aerial photographs" -> dcmiType.Image,
    "albumen prints" -> dcmiType.Image,
    "appliance" -> dcmiType.PhysicalObject,
    "architectural drawing" -> dcmiType.Image,
    "art print" -> dcmiType.Image,
    "audio" -> dcmiType.Sound,
    "audiofile" -> dcmiType.Sound,
    "black and white postcard" -> dcmiType.Image,
    "book" -> dcmiType.Text,
    "books and documents" -> dcmiType.Text,
    "bookchapter" -> dcmiType.Text,
    "caricatures" -> dcmiType.Image,
    "cartographic material" -> dcmiType.Image,
    "cartographic" -> dcmiType.Image,
    "cartoons" -> dcmiType.Image,
    "certificate" -> dcmiType.Text,
    "codices" -> dcmiType.Text,
    "collodion negative" -> dcmiType.Image,
    "color slide" -> dcmiType.Image,
    "container" -> dcmiType.PhysicalObject,
    "copy photograph" -> dcmiType.Image,
    "correspondence" -> dcmiType.Text,
    "costume plate" -> dcmiType.Image,
    "costume"  -> dcmiType.PhysicalObject,
    "cyanotypes" -> dcmiType.Image,
    "deposition" -> dcmiType.Text,
    "diaries" -> dcmiType.Text,
    "diary" -> dcmiType.Text,
    "drawing" -> dcmiType.Image,
    "drawings--reproductions" -> dcmiType.Image,
    "dry plate negatives" -> dcmiType.Image,
    "electronic component" -> dcmiType.PhysicalObject,
    "electronic resource" -> dcmiType.InteractiveResource,
    "email message" -> dcmiType.Text,
    "engraving" -> dcmiType.Image,
    "equipment" -> dcmiType.PhysicalObject,
    "essay" -> dcmiType.Text,
    "film" -> dcmiType.MovingImage,
    "finding aid" -> dcmiType.Collection,
    "frame"  -> dcmiType.PhysicalObject,
    "furnishing" -> dcmiType.PhysicalObject,
    "furniture"  -> dcmiType.PhysicalObject,
    "glass negative" -> dcmiType.Image,
    "glass negatives" -> dcmiType.Image,
    "glass plate negative" -> dcmiType.Image,
    "glass transparencies" -> dcmiType.Image,
    "handbook" -> dcmiType.Text,
    "illumination" -> dcmiType.Image,
    "image" -> dcmiType.Image,
    "images" -> dcmiType.Image,
    "interactiveResources" -> dcmiType.InteractiveResource,
    "jewelry"  -> dcmiType.PhysicalObject,
    "journal" -> dcmiType.Text,
    "landscape photographs" -> dcmiType.Image,
    "landscapes" -> dcmiType.Image,
    "ledger" -> dcmiType.Text,
    "letter" -> dcmiType.Text,
    "letters (correspondence)" -> dcmiType.Text,
    "log" -> dcmiType.Text,
    "magazine" -> dcmiType.Text,
    "manual" -> dcmiType.Text,
    "manuscript" -> dcmiType.Text,
    "map" -> dcmiType.Image,
    "memorandum" -> dcmiType.Text,
    "minutes" -> dcmiType.Text,
    "mixed material" -> dcmiType.Image,
    "monochromatic" -> dcmiType. Image,
    "motion picture" -> dcmiType.MovingImage,
    "moving image" -> dcmiType.MovingImage,
    "movingimage" -> dcmiType.MovingImage,
    "movingimages" -> dcmiType.MovingImage,
    "nautical charts" -> dcmiType.Image,
    "negative" -> dcmiType.Image,
    "newsletter" -> dcmiType.Image,
    "nitrate negatives" -> dcmiType.Image,
    "notated music" -> dcmiType.Image,
    "object" -> dcmiType.PhysicalObject,
    "online collection" -> dcmiType.Collection,
    "online exhibit" -> dcmiType.InteractiveResource,
    "online text" -> dcmiType.Text,
    "oral history recording" -> dcmiType.Sound,
    "painting" -> dcmiType.Image,
    "paintings--reproductions" -> dcmiType.Image,
    "pamphlet" -> dcmiType.Text,
    "panoramic photographs" -> dcmiType.Image,
    "periodical" -> dcmiType.Text,
    "photo, print, drawing" -> dcmiType.Image,
    "photograph" -> dcmiType.Image,
    "photographic postcard" -> dcmiType.Image,
    "photomechanical print" -> dcmiType.Image,
    "physical object" -> dcmiType.PhysicalObject,
    "PhysicalObject" -> dcmiType.PhysicalObject,
    "portraits" -> dcmiType.Image,
    "postcard" -> dcmiType.Image,
    "poster" -> dcmiType.Image,
    "press release" -> dcmiType.Text,
    "print" -> dcmiType.Image,
    "printed graphic" -> dcmiType.Image,
    "publication" -> dcmiType.Text,
    "receipt" -> dcmiType.Text,
    "recording" -> dcmiType.Sound,
    "report" -> dcmiType.Text,
    "sample book"  -> dcmiType.PhysicalObject,
    "sculpture"  -> dcmiType.PhysicalObject,
    "self-portraits" -> dcmiType.Image,
    "sheet music" -> dcmiType.Image,
    "silver gelatin glass transparencies" -> dcmiType.Image,
    "silver gelatin prints" -> dcmiType.Image,
    "slide" -> dcmiType.Image,
    "sound" -> dcmiType.Sound,
    "sound recording" -> dcmiType.Sound,
    "sounds" -> dcmiType.Sound,
    "specimen"  -> dcmiType.PhysicalObject,
    "statue"  -> dcmiType.PhysicalObject,
    "still image" -> dcmiType.Image,
    "stillimage" -> dcmiType.Image,
    "technical drawing" -> dcmiType.Image,
    "text" -> dcmiType.Text,
    "textile"  -> dcmiType.PhysicalObject,
    "three dimensional artifact" -> dcmiType.PhysicalObject,
    "tool"  -> dcmiType.PhysicalObject,
    "transcript" -> dcmiType.Text,
    "transparency" -> dcmiType.Image,
    "video game" -> dcmiType.InteractiveResource,
    "video" -> dcmiType.MovingImage,
    "writing" -> dcmiType.Text,
    "written document" -> dcmiType.Text,
    "written" -> dcmiType.Text
  )

  /**
    * Create a Map[String,String] from the DcmiTypeMap[String, IRI]
    * mapping the non-standard term to a an appropriate web label
    * representation of the IRI. The final map is loaded into
    * `typeLookup`.
    *
    * Example:
    * ('tools' -> DcmiType.Image) -> ('tools' -> 'image')
    * ('statue' -> DcmiType.PhysicalObject) -> ('statue' -> 'physical object')
    *
    * @return
    */
  private def loadVocab = addVocab(DcmiTypeMap)

  // Load vocab
  loadVocab

  /**
    * Gets the web label representation of the given IRI
    *
    * @param iri
    * @return String
    */
  private def getTypeLabel(iri: IRI): String = { iri match {
      case dcmiType.InteractiveResource => "interactive resource"
      case dcmiType.MovingImage => "moving image"
      case dcmiType.PhysicalObject => "physical object"
      case dcmiType.StillImage => "still image"
      case _ => iri.getLocalName.toLowerCase()
    }
  }

  /**
    * Convert a Map[String,IRI] to Map[String,String]
    *
    * @param vocabMap Map[String,IRI]
    * @return Map[String,String]
    */
  private def convertMap(vocabMap: Map[String, IRI]): Map[String,String] =
    vocabMap.map(p => p._1 -> getTypeLabel(p._2))

  /**
    * Add a vocab map to `typeLookup` vocabulary
    *
    * @param vocabulary Map[String,Any]
    */
  //noinspection TypeAnnotation
  def addVocab(vocabulary: Map[String, Any]) = vocabulary match {
    case iri: Map[String, IRI] => typeLookup.add(convertMap(iri))
    case str: Map[String, String] => typeLookup.add(str)
  }

  /**
    * Find the original value in the controlled vocabulary
    *
    * @param value Original value
    * @return T Value from the controlled vocabulary if found
    */
  override def enrich(value: String): Option[String] =
    typeLookup.lookup(value)
}
