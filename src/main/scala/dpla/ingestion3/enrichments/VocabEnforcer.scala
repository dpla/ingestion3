package dpla.ingestion3.enrichments

import dpla.ingestion3.mappers.rdf.DCMIType
import org.eclipse.rdf4j.model.IRI


trait VocabEnforcer[T] {
  val enforceVocab: (T, Set[T]) => Boolean = (value, vocab) => {
    // TODO: Should this be case insensitive? It will fail on 'image' but pass on 'Image"
    vocab.contains(value)
  }

  val mapVocab: (T, Map[T, IRI]) => Option[IRI] = (value, vocab) => {
    // TODO string similarity comparision, this just does a strict lookup...
    vocab.get(value)
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

  println(DcmiTypeStrings)

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

  val mapDcmiType: (String) => Any = mapVocab(_, DcmiTypeMap)
}
