package dpla.ingestion3.enrichments
import dpla.ingestion3.mappers.rdf.DCMIType
import org.eclipse.rdf4j.model.IRI

class TypeMapper extends VocabMapper[String] {

  private val typeLookup = new MapperLookup[String](
    (term: String) => normalizationFunc(term)
  )

  private def normalizationFunc(term: String): String = term.toLowerCase.trim

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
    "stillimage" -> dcmiType.Image,
    "text" -> dcmiType.Text,
    "textile" -> dcmiType.Image,
    "tool" -> dcmiType.Image,
    "video game" -> dcmiType.InteractiveResource,
    "video" -> dcmiType.MovingImage,
    "writing" -> dcmiType.Text,
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

  private def convertMap(map: Map[String, IRI]): Map[String,String] =
    map.map(p => p._1 -> getTypeLabel(p._2))


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
