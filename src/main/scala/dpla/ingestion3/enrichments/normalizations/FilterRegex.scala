package dpla.ingestion3.enrichments.normalizations

/** Standardized regular expression templates used to create block and allow
  * patterns.
  */
object FilterRegex {

  implicit class Regex(value: String) {

    /** A regex appropriate for removing all occurrences stop word from the
      * original value.
      *
      * Matches on: 1) Case insensitive 2) Start of string a) Ignores all
      * leading white space 3) End of string a) Ignores all trailing white space
      *
      * Matches when using 'jpeg' for a block term
      *   - 'jpeg' -> 'jpeg'
      *   - ' jpeg ' -> 'jpeg'
      *   - 'JPEG' -> 'JPEG'
      *
      * TODO Support for plurals
      * TODO Support for ignoring punctuation separating terms
      * TODO Support for multi-line matching
      */
    val blockListRegex: String = """(?i)^\s*""" + escape(value) + """\s*$"""

    /** A regex for retaining terms on an allowed list
      *
      * Examples with allowList=['film','image'] (original -> filtered)
      *   - film DVD -> film
      *   - image and picture -> image
      *
      * TODO Term filtering within string
      * TODO Term filtering within string (ignore commas)
      * TODO Term filtering within string (extra internal
      * whitespace)
      */
    /*
     FIXME
      This matches the blockListRegex (for now) because the matching requirements are
      fairly straight forward but as they get more complex it will probably be necessary
      to have different regexs when identifying patterns to exclude and patterns to retain
     */
    val allowListRegex: String = """(?i)^\s*""" + escape(value) + """\s*$"""

    /** Escapes reserved regular expression characters in string
      *
      * @param string
      *   Original value
      * @return
      *   String
      */
    // TODO is there a better way to escape reserved regex chars?
    private def escape(string: String): String =
      string
        .replace("""\""", """\\""")
        .replace("""/""", """\/""")
        .replace("""+""", """\+""")
        .replace("""-""", """\-""")
  }
}
