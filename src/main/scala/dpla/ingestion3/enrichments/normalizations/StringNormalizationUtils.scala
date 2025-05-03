package dpla.ingestion3.enrichments.normalizations

import dpla.ingestion3.enrichments.normalizations.filters.{
  ExtentExceptionsList,
  ExtentIdentificationList
}
import org.apache.commons.text.StringEscapeUtils
import org.jsoup.Jsoup
import org.jsoup.nodes.Document.OutputSettings
import org.jsoup.nodes.Entities.EscapeMode
import org.jsoup.safety.Safelist

import java.util.regex.Pattern

/** String enrichments
  */
object StringNormalizationUtils {

  implicit class Normalizations(stringToBeNormalized: String) {

    private type SingleStringEnrichment = String

    /** Accepts a set of Strings to preserve in the original value. Those string
      * values can be literal strings or regular expressions. Regular
      * expressions can allow for filtering out terms while:
      *   - ignoring white space
      *   - ignoring case
      *   - ignoring punctuation
      *   - Greedy matching on plurals
      *
      * Construction and testing of those regular expressions is dependent on
      * the user
      */
    lazy val applyAllowFilter: Set[String] => String = allowList =>
      allowList.map(Pattern.compile(_, Pattern.CASE_INSENSITIVE).matcher(stringToBeNormalized))
        .filter(_.find())
        .map(_ => stringToBeNormalized)
        .mkString(" ").reduceWhitespace


    /** Applies the provided set of patterns and removes all matches from the
      * original string
      */
    lazy val applyBlockFilter: Set[String] => String = termList =>
      termList
        .foldLeft(stringToBeNormalized) { case (acc, pattern) =>
          acc.replaceAll(pattern, "").reduceWhitespace
        }

    /** Find and capitalize the first character in a given string
      *
      * 3 blind mice -> 3 blind mice three blind mice -> Three blind mice.
      * ...vacationland... -> ...Vacationland... The first bike -> The first
      * bike
      */
    lazy val capitalizeFirstChar: SingleStringEnrichment = {
      val charIndex = findFirstChar(stringToBeNormalized)
      if (charIndex >= 0)
        replaceCharAt(
          stringToBeNormalized,
          charIndex,
          stringToBeNormalized.charAt(charIndex).toUpper
        )
      else
        stringToBeNormalized
    }

    /** Removes trailing colons, semicolons, commas, slashes, hyphens and
      * whitespace characters (whitespace, tab, new line and line feed) that
      * follow the last letter or digit
      */
    lazy val cleanupEndingPunctuation: SingleStringEnrichment = {
      // FIXME rewrite as a regular expression
      val endIndex = stringToBeNormalized.lastIndexWhere(_.isLetterOrDigit)
      if (endIndex == -1) {
        // If there is nothing to clean up then return the existing string
        stringToBeNormalized
      } else {
        val start = stringToBeNormalized.substring(0, endIndex + 1)
        val end = stringToBeNormalized.substring(endIndex + 1)
        val cleanEnd = end.replaceAll(beginAndEndPunctuationToRemove, "")
        start.concat(cleanEnd)
      }
    }

    /** Removes trailing spaces and commas
      */
    lazy val cleanupEndingCommaAndSpace: SingleStringEnrichment = {
      val endIndex = stringToBeNormalized.lastIndexWhere(_.isLetterOrDigit)
      if (endIndex == -1) {
        // If there is nothing to clean up then return the existing string
        stringToBeNormalized
      } else {
        val start = stringToBeNormalized.substring(0, endIndex + 1)
        val end = stringToBeNormalized.substring(endIndex + 1)
        val cleanEnd = end.replaceAll("""[,\s]""", "")
        start.concat(cleanEnd)
      }
    }

    /** Removes leading colons, semicolons, commas, slashes, hyphens and
      * whitespace characters (whitespace, tab, new line and line feed) that
      * precede the first letter or digit
      */
    lazy val cleanupLeadingPunctuation: SingleStringEnrichment = {
      val beginIndex = stringToBeNormalized.indexWhere(_.isLetterOrDigit)
      if (beginIndex == -1)
        stringToBeNormalized
      else
        stringToBeNormalized
          .substring(0, beginIndex)
          .replaceAll(beginAndEndPunctuationToRemove, "")
          .concat(stringToBeNormalized.substring(beginIndex))
    }

    /** Accepts a String value and splits it around periods. Strips trailing and
      * leading whitespace from those "sentences" and capitalizes the first
      * character of each sentence leaving all other characters alone.
      *
      * We do not assume that all upper case characters should be down-cased.
      */
    lazy val convertToSentenceCase: SingleStringEnrichment = {
      val pattern: scala.util.matching.Regex = """.*?(\.)""".r
      val sentences =
        for (t <- pattern findAllIn stringToBeNormalized)
          yield t.trim.capitalize
      // rejoin the sentences and add back the whitespace that was trimmed off
      sentences.mkString(" ")
    }

    /** */
    lazy val extractExtents: SingleStringEnrichment =
      stringToBeNormalized
        // allow things that look like extents
        .applyAllowFilter(ExtentIdentificationList.termList)
        // block exceptions
        .applyBlockFilter(ExtentExceptionsList.termList)

    /** Truncates the string at the specified length
      */
    lazy val limitCharacters: Int => String = length =>
      if (stringToBeNormalized.length > length)
        stringToBeNormalized.substring(0, length)
      else stringToBeNormalized

    /** Reduce multiple whitespace values to a single and removes leading and
      * trailing white space
      *
      * https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
      * \h A horizontal whitespace character: [
      * \t\xA0\u1680\u180e\u2000-\u200a\u202f\u205f\u3000] \v A vertical
      * whitespace character: [\n\x0B\f\r\x85\u2028\u2029]
      */
    lazy val reduceWhitespace: SingleStringEnrichment =
      stringToBeNormalized.replaceAll("[\\h\\v]+", " ").trim

    lazy val cleanupGeocoordinates: SingleStringEnrichment =
      stringToBeNormalized.splitAtDelimiter(",") match {
        case split: Array[String] if split.length == 2 =>
          val newNorth = split(0).replaceAll("[nN]$", "")
          val newWest = split(1).replaceAll("[wW]$", "")
          val newCoordinates = f"$newNorth, $newWest"
          if (
            newCoordinates.matches(
              "^(\\-?\\d+(\\.\\d+)?),\\s*(\\-?\\d+(\\.\\d+)?)$"
            )
          )
            newCoordinates
          else
            ""
        case _ => ""
      }

    /** Splits a String value around a given delimiter.
      */
    lazy val splitAtDelimiter: String => Array[String] = delimiter => {
      stringToBeNormalized.split(delimiter).map(_.trim).filter(_.nonEmpty)
    }

    /** If string does not contain an opening bracket, strip all closing
      * brackets.
      */
    lazy val stripUnmatchedClosingBrackets: SingleStringEnrichment =
      if (stringToBeNormalized.contains("[")) stringToBeNormalized
      else stringToBeNormalized.replace("]", "")

    /** If string does not contain a closing bracket, strip all opening
      * brackets.
      */
    lazy val stripUnmatchedOpeningBrackets: SingleStringEnrichment =
      if (stringToBeNormalized.contains("]")) stringToBeNormalized
      else stringToBeNormalized.replace("[", "")

    /** Strip all double quotes from the given string
      */
    lazy val stripDblQuotes: SingleStringEnrichment =
      stringToBeNormalized.replaceAll("\"", "")

    lazy val stripHTML: SingleStringEnrichment = {
      val unescaped = StringEscapeUtils.unescapeHtml4(stringToBeNormalized)
      val cleaned = Jsoup.clean(
        unescaped,
        "",
        Safelist.none(),
        new OutputSettings().escapeMode(EscapeMode.xhtml)
      )
      StringEscapeUtils.unescapeHtml4(cleaned)
    }

    /** Removes singular period from the end of a string. Ignores and removes
      * trailing whitespace
      */
    lazy val stripEndingPeriod: SingleStringEnrichment =
      if (stringToBeNormalized.matches(""".*?[^\.]\.[\n\r\s]*$"""))
        stringToBeNormalized.replaceAll("""\.[\n\r\s\h\v]*$""", "")
      else
        stringToBeNormalized

    /** Punctuation and whitespace characters that should be removed from
      * leading and trailing parts of string
      *
      * semicolon (;) colon (:) slash (/) comma (,) hyphen (-) new line (\n) tab
      * (\t) carriage return (\r) whitespace (\s)
      */
    private def beginAndEndPunctuationToRemove = """[-;:,\/\\t\\r\\n\s]"""

    /** Iterates over the string to find the first alphanumeric char and returns
      * the index
      *
      * @param str
      *   String to search
      * @return
      */
    private def findFirstChar(str: String): Int =
      str.indexWhere(_.isLetterOrDigit)

    /** Replaces the character at the specified index in s with c
      *
      * @param s
      *   String value
      * @param pos
      *   Index
      * @param c
      *   New char
      * @return
      *   A new String with Char c at index pos
      */
    private def replaceCharAt(s: String, pos: Int, c: Char): String =
      s.substring(0, pos) + c + s.substring(pos + 1)
  }
}
