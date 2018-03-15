package dpla.ingestion3.enrichments

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
  * Standardized regular expression templates used to create block and allow patterns.
  */
object FilterRegex {
  implicit class Regex(value: String) {
    /**
      * A regex appropriate for removing all occurrences stop word from the original value.
      *
      *  Matches on:
      *   1) Case insensitive
      *   2) Starts with one or more of any:
      *     a) Word boundary (\b)
      *     c) Alphanumeric (a-zA-Z0-9)
      *   3) Ends with one ore more of either
      *     a) 's' OR comma (s|,)+
      *     b) white space OR end of string (\s+|$)+
      *
      * Matches when using 'jpeg' for a block term
      *   - 'jpeg  ' -> 'jpeg  '
      *   - 'jpeg photograph' -> 'jpeg '
      *   - 'jpeg photograph jpeg' -> 'jpeg  jpeg'
      *
      * TODO Work with Gretchen to properly define this and any other matching regex
      *   - Follow-up 3/16
      * TODO Support for plurals
      * TODO Support for ignoring punctuation separating terms
      *   - Remove trailing punctuation or apply split() e.g. fixup trailing comma here > 'film, dvd' -> 'film,'
      *   - Identify all valid word separating punctuation (e.x. /+\- are not valid word terminators)
      * TODO Support for multi-line matching
      *
      */
    // Original version
    // val blockListRegex: String = """(?i)((\b|\s+|[a-zA-z0-9]+)""" + escapeRegex(value) + """((s|,)+|\s+|$))"""
    val blockListRegex: String = """(?i)((\b|\s+|[a-zA-z0-9]+)""" + escapeRegex(value) + """(\s+|\b))"""

    /**
      *
      */
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
