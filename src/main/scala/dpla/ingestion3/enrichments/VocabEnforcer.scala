package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.DcmiTypeEnforcer.dcmiType
import dpla.ingestion3.mappers.rdf.DCMIType
import dpla.ingestion3.model.SkosConcept
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

  /**
    * FIXME This doesn't work with MapVocab b/c the
    * Accepts a SkosConcept from the mapped record and attempts to lookup the mapped value
    * in an external vocabulary returning the preferred form.
    *
    *  If the lookup performed above returned a match then we need to merge the providedLabel value
    *  from originalValue with the enriched concept and scheme values. If no match was found these
    *  step are essentially no-ops (setting None values).
    *
    *  For languages: The concept property should only contain ISO-639 term values. If not match is
    *  found then it should remain blank and the original value remains in providedLabel.
    */
  val matchToSkosVocab: (SkosConcept, Map[String,String]) => SkosConcept = (originalValue, skosVocab) => {
    val lcTerm = originalValue.providedLabel.getOrElse("").toLowerCase()
    val enrichedLexvo = skosVocab.get(lcTerm)

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
    "stillimage" -> dcmiType.Image,
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

  // FIXME since these are already overriding the IRI label why can't they
  // resolve to lower case values (@see the type enrichment _.toLowerCase)
  val mapDcmiTypeString: (IRI) => String = {
    case dcmiType.InteractiveResource => "InteractiveResource"
    case dcmiType.MovingImage => "Moving Image"
    case dcmiType.PhysicalObject => "Physical Object"
    case dcmiType.StillImage => "Still Image"
    case iri => iri.getLocalName
  }
}