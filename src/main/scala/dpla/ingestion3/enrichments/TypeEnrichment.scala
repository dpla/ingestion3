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
    "appliance" -> dcmiType.PhysicalObject,
    "article" -> dcmiType.Text,
    "audio" -> dcmiType.Sound,
    "Book" -> dcmiType.Text,
    "cartographic" -> dcmiType.Image,
    "certificate" -> dcmiType.Text,
    "collodion" -> dcmiType.Image,
    "color slide" -> dcmiType.Image,
    "container" -> dcmiType.PhysicalObject,
    "Correspondence" -> dcmiType.Text,
    "costume" -> dcmiType.PhysicalObject,
    "Deposition" -> dcmiType.Text,
    "Diaries" -> dcmiType.Text,
    "Diary" -> dcmiType.Text,
    "document" -> dcmiType.Text,
    "drawing" -> dcmiType.Image,
    "electronic resource" -> dcmiType.InteractiveResource,
    "Email message" -> dcmiType.Text,
    "equipment" -> dcmiType.PhysicalObject,
    "Essay" -> dcmiType.Text,
    "film" -> dcmiType.MovingImage,
    "frame" -> dcmiType.PhysicalObject,
    "furniture" -> dcmiType.PhysicalObject,
    "illumination" -> dcmiType.Image,
    "image" -> dcmiType.Image,
    "jewelry" -> dcmiType.PhysicalObject,
    "Journal" -> dcmiType.Text,
    "ledger" -> dcmiType.Text,
    "letter" -> dcmiType.Text,
    "Logs" -> dcmiType.Text,
    "Magazine" -> dcmiType.Text,
    "Manual" -> dcmiType.Text,
    "Manuscript" -> dcmiType.Text,
    "Memorandum" -> dcmiType.Text,
    "mixed material" -> dcmiType.Image,
    "monochromatic" -> dcmiType.Image,
    "motion picture" -> dcmiType.MovingImage,
    "moving image" -> dcmiType.MovingImage,
    "movingimage" -> dcmiType.MovingImage,
    "negative" -> dcmiType.Image,
    "notated music" -> dcmiType.Image,
    "object" -> dcmiType.PhysicalObject,
    "online exhibit" -> dcmiType.InteractiveResource,
    "oral history recording" -> dcmiType.Sound,
    "painting" -> dcmiType.Image,
    "Pamphlet" -> dcmiType.Text,
    "Periodical" -> dcmiType.Text,
    "photograph" -> dcmiType.Image,
    "physical object" -> dcmiType.PhysicalObject,
    "physicalobject" -> dcmiType.PhysicalObject,
    "postcard" -> dcmiType.Image,
    "poster" -> dcmiType.Image,
    "Press release" -> dcmiType.Text,
    "print" -> dcmiType.Image,
    "Publication" -> dcmiType.Text,
    "Receipt" -> dcmiType.Text,
    "recording" -> dcmiType.Sound,
    "report" -> dcmiType.Text,
    "sample book" -> dcmiType.PhysicalObject,
    "sculpture" -> dcmiType.PhysicalObject,
    "sound" -> dcmiType.Sound,
    "specimen" -> dcmiType.PhysicalObject,
    "statue" -> dcmiType.PhysicalObject,
    "still image" -> dcmiType.Image,
    "stillimage" -> dcmiType.Image,
    "Text" -> dcmiType.Text,
    "textile" -> dcmiType.PhysicalObject,
    "tool" -> dcmiType.PhysicalObject,
    "transcript" -> dcmiType.Text,
    "video game" -> dcmiType.InteractiveResource,
    "video" -> dcmiType.MovingImage,
    "Writing" -> dcmiType.Text,
    "Written" -> dcmiType.Text
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
