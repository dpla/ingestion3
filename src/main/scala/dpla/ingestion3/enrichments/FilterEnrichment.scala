package dpla.ingestion3.enrichments

import dpla.ingestion3.utils.FlatFileIO


/**
  * A abstraction for filtering values. These are lists of terms or regular expressions that
  * are to be removed from the original values.
  *
  * This trait is implemented for a number of base filters (DigitalSurrogateBlockList,
  * FormatTypeBlockList) but can be extended in a provider mapping with a custom term list.
  *
  * // Example custom block list
  * import dpla.ingestion3.enrichments.FilterRegex._
  * object ProviderBlockList extends FilterList {
  *   override val termList = Set[...].map(_.blockListRegex)
  * }
  *
  */
trait FilterList {
  // Set of terms to filter around (either block list or allow list)
  val termList: Set[String]

  // File paths to source termList from
  val files: Seq[String] = Seq("")

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

/**
  * Stores standardized regular expressions used to create block and allow patterns
  */
object FilterRegex {
  implicit class Regex(value: String) {
    // A regex appropriate for removing a stop word from the original value
    val blockListRegex: String = """(?i)((^|\s|,|s+)""" + escapeRegex(value) + """($|\s|s|,+))"""

    val allowListRegex: String = """(?i)((?<=(^|\s+))""" + escapeRegex(value) + """(?=($|\s+)))"""

    /**
      * Escapes reserved regular expression characters in string
      *
      * @param string Original value
      * @return String
      */
    // TODO is there a better way to escape reserved regex chars?
    private def escapeRegex(string: String): String =
      string
        .replace("""/""", """\/""")
        .replace("""+""", """\+""")
        .replace("""-""", """\-""")
  }
}
