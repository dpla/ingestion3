package dpla.ingestion3.enrichments.normalizations.filters

import dpla.ingestion3.enrichments.normalizations.FilterRegex._
import dpla.ingestion3.enrichments.normalizations.FilterList

/**
  * Type terms that should be removed from the format field
 *
  * @see https://docs.google.com/document/d/1sdYM8INg-jBEPc-RcSib4oD5QZLrB6zwU6CfPRukG1M/edit
  */
object FormatTypeValuesBlockList extends FilterList {
  override val termList: Set[String] = Set(
    "Image",
    "Still image",
    "Stillimage",
    "Sound",
    "Audio",
    "Text",
    "Moving image",
    "Movingimage",
    "Object",
    "Physical object",
    "Physicalobject",
    "Interactive resource",
    "interactiveresource")
      .map(_.blockListRegex)
}
