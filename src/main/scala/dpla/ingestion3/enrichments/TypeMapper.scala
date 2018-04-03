package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.DcmiTypeEnforcer.dcmiType
import org.eclipse.rdf4j.model.IRI

class TypeMapper extends VocabMapper[String] {

  private val typeLookup = new MapperLookup[String](
    (term: String) => normalizationFunc(term),
    (prov: String, enriched: String) => enriched // Merge function is not needed for type mapper
  )

  private def normalizationFunc(term: String): String = term.toLowerCase().trim

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

  // Load vocab
  loadVocab

  private def mapDcmiTypeString(iri: IRI): String = iri match
  {
    case dcmiType.InteractiveResource => "interactive resource"
    case dcmiType.MovingImage => "moving image"
    case dcmiType.PhysicalObject => "physical object"
    case dcmiType.StillImage => "still image"
    case _ => iri.getLocalName.toLowerCase()
  }

  private def loadVocab =
    typeLookup.add(
      DcmiTypeMap.map(p => p._1 -> mapDcmiTypeString(p._2))
    )

  override def enrich(value: String): Option[String] =
    typeLookup.lookup(value)
}
