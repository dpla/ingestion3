package dpla.ingestion3.enrichments.normalizations

import dpla.ingestion3.utils.FlatFileIO

/**
  * A abstraction for filtering values. These are lists of terms or regular expressions that
  * are to be removed from the original values.
  *
  * This trait is implemented for a number of base filters (DigitalSurrogateBlockList,
  * FormatTypeBlockList) but can be extended in a provider mapping with a custom term list.
  *
  * Example custom block list
  *
  *   import dpla.ingestion3.enrichments.FilterRegex._
  *
  *   object ProviderBlockList extends FilterList {
  *     override val termList = Set[...].map(_.blockListRegex)
  *   }
  *
  */
trait FilterList {
  // Set of terms to filter around (either block list or allow list)
  val termList: Set[String]

  // File paths to source termList from
  val files: Seq[String] = Seq()

  /**
    * Reads files listed in `files` member. Ignores lines that begin with '#'
    * @return Set[String] Unique set of terms in files
    */
  def getTermsFromFiles: Set[String] = {
    val fileIo = new FlatFileIO()
    files
      .flatMap(file => fileIo.readFileAsSeq(file))
      .filterNot(line => line.startsWith("#"))
      .toSet
  }
}
