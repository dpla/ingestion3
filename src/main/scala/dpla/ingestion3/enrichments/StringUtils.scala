package dpla.ingestion3.enrichments

import dpla.ingestion3.utils.FlatFileIO
import org.apache.commons.lang.StringEscapeUtils
import org.jsoup.Jsoup
import org.jsoup.nodes.Document.OutputSettings
import org.jsoup.nodes.Entities.EscapeMode
import org.jsoup.safety.Whitelist

import scala.util.matching.Regex

/**
  * String enrichments
  *
  */
object StringUtils {
  implicit class Enrichments(value: String) {

    type SingleStringEnrichment = String
    type MultiStringEnrichment = Array[String]

    /**
      * Accepts a String value and splits it around periods. Strips
      * trailing and leading whitespace from those "sentences" and
      * capitalizes the first character of each sentence leaving all
      * other characters alone.
      *
      * We do not assume that all upper case characters should be
      * downcased.
      *
      */
    val convertToSentenceCase: SingleStringEnrichment = {
      val pattern: Regex = """.*?(\.)""".r
      val sentences = for( t <- pattern findAllIn value) yield t.trim.capitalize
      // rejoin the sentences and add back the whitespace that was trimmed off
      sentences.mkString(" ")
    }

    val limitCharacters: (Int) => (String) = (length) => {
      if (value.length > length) value.substring(0, length)
      else value
    }

    /**
      * Splits a String value around a given delimiter.
      */
    val splitAtDelimiter: (String) => Array[String] = (delimiter) => {
      value.split(delimiter).map(_.trim).filter(_.nonEmpty)
    }

    /**
      * Removes leading and trailing: colons, semi-colons, commas
      * slashes, hyphens and whitespace characters (whitespace, tab, new line and line feed)
      *
      */
    val cleanupEndingPunctuation: SingleStringEnrichment = {
      // FIXME this is going to be problematic if the last character
      // is valid but not alphanumeric (e.g. ends with a " or ')
      val endIndex = value.lastIndexWhere(_.isLetterOrDigit)


      if (endIndex == -1)
        // If there is nothing to cleanup then return the existing string
        value
      else {
        val start = value.substring(0, endIndex + 1)
        val cleanEnd = value
          .substring(endIndex+1)
          .replaceAll(beginAndEndPunctuationToRemove, "")
        start.concat(cleanEnd)
      }
    }

    val cleanupLeadingPunctuation: SingleStringEnrichment = {
      val beginIndex = value.indexWhere(_.isLetterOrDigit)

      if (beginIndex == -1)
        // If there is nothing to cleanup than return the original string
        value
      else
        value
          .substring(0, beginIndex)
          .replaceAll(beginAndEndPunctuationToRemove, "")
          .concat(value.substring(beginIndex))
    }

    val stripHTML: SingleStringEnrichment = {
      val unescaped = StringEscapeUtils.unescapeHtml(value)
      val cleaned = Jsoup.clean(unescaped, "", Whitelist.none(), new OutputSettings().escapeMode(EscapeMode.xhtml))
      StringEscapeUtils.unescapeHtml(cleaned)
    }


    /**
      * Removes singular period from the end of a string. Ignores and removes trailing whitespace
      */
    val stripEndingPeriod: SingleStringEnrichment = {
      if (value.matches(""".*?[^\.]\.[\n\r\s]*$"""))
        value.replaceAll("""\.[\n\r\s]*$""", "")
      else
        value
    }

    /**
      * Reduce multiple whitespace values to a single
      */
    val reduceWhitespace: SingleStringEnrichment = {
      value.replaceAll(" +", " ")
    }

    /**
      * Find and capitalize the first character in a given string
      *
      * 3 blind mice -> 3 blind mice
      * three blind mice -> Three blind mice.
      * ...vacationland... -> ...Vacationland...
      *   The first bike ->   The first bike
      *
      * @return
      */
    val capitalizeFirstChar: SingleStringEnrichment = {
      val charIndex = findFirstChar(value)
      if (charIndex >= 0)
        replaceCharAt(value, charIndex, value.charAt(charIndex).toUpper)
      else
        value
    }

    /**
      * Replaces the character at the specified index in s with c
      * @param s String value
      * @param pos Index
      * @param c New char
      * @return A new String with Char c at index pos
      */
    private def replaceCharAt(s: String, pos: Int, c: Char): String = s.substring(0, pos) + c + s.substring(pos + 1)

    /**
      * Iterates over the string to find the first alphanumeric char
      * and returns the index
      *
      * @param str String to search
      * @return
      */
    private def findFirstChar(str: String): Int = {
      str.indexWhere(_.isLetterOrDigit)
    }

    /**
      * Punctuation and whitespace characters that should be removed
      * from leading and trailing parts of string
      *
      *   semicolon (;)
      *   colon (:)
      *   slash (/)
      *   comma (,)
      *   hyphen (-)
      *   new line (\n)
      *   tab (\t)
      *   carriage return (\r)
      *   whitespace (\s)
      */
    private def beginAndEndPunctuationToRemove = """[;:/,-\\t\\r\\n\s]"""


    /**
      * Bracket removal normalization
      *
      * Removes matching leading and trailing brackets (square, round and curly braces)
      * from a string
      */
    val stripBrackets: SingleStringEnrichment = {
      val replacementRegex = for( (p, r) <- bracketPatterns if value.matches(p)) yield r
      // TODO is there a cleaner or safer way to convert Iterator[String] to String than mkString?
      value.replaceAll(replacementRegex.mkString, "")
    }

    /**
      * Bracket patterns to search for and the corresponding replacement regex
      *
      * E.x.
      *   pattern to search for -> pattern to replace matches
      *   if it starts with ( and ends with ) -> get the first ( and last )
      *   """^(\(.*\)$)""" -> """^(\()|(\))$"""
      *
      *
      * @return Map[String, String]
      */
    private def bracketPatterns = {
      brackets.map(bracket => baseBracketSearchPattern(bracket) -> baseBracketReplacePattern(bracket))
    }

    /**
      * Constructs the regex pattern to search a string
      *
      * @param bracket Bracket pairs to search for
      * @return String Regex
      */
    def baseBracketSearchPattern(bracket: Bracket): String =
      """^([\n\r\s]*\""" + bracket.openChar + """.*\""" + bracket.closeChar + """[\n\r\s]*)"""

    /**
      * Constructs the regex pattern to perform a replacement
      *
      * @param bracket Bracket pairs to search for
      * @return String Regex
      */
    private def baseBracketReplacePattern(bracket: Bracket) =
      """^([\n\r\s]*\""" + bracket.openChar + """[\n\r\s]*)|([\n\r\s]*\""" + bracket.closeChar + """[\n\r\s]*)$"""

    private def brackets = Seq(
      Bracket("{","}"),
      Bracket("(",")"),
      Bracket("[","]")
    )

    /**
      * Case class that represents a pair of enclosing strings that
      * @param openChar String that indicates start of enclosure
      * @param closeChar String that indicates end of enclosure
      */
    case class Bracket(openChar: String, closeChar: String)


    /**
      * Returns a list of files to use for generating the format stop words list
      *
      * @return
      */
    // TODO These file references should not be hard coded
    // TODO See VocabEnforcer -- there is some duplicate functionality that could be cleaned up
    private def formatStopwordFiles = Seq(
      "/formats/ohio.csv",
      "/formats/iana-imt-types.csv"
    )

    /**
      * Reads files listed in formatStopwordFiles and builds an
      * ordered Set[String] (ordered by white space count descending)
      *
      * @return Set[String]
      */
    private def getFormatStopwords: Set[String] = {
      val fileIo = new FlatFileIO()
      // reads in terms from file
      val stopWords = formatStopwordFiles
        .flatMap(file => fileIo.readFileAsSeq(file))
        .filterNot(line => line.startsWith("#")) // drop lines that begin with #

      // Order the stop words by number of whitespaces. This will create term
      // precedence when constructing the search regex so that 'jpeg 2000' will
      // be found and replaced before 'jpeg'.
      stopWords
        .map(term => term.count(_ == ' ')) // count the number of white spaces
        .zip(stopWords) // zip white space count and term
        .sorted // sort white space count ascending
        .reverse // sort white space count descending
        .map(_._2) // pull out the term
        .map(
          // TODO is there a better way to escape reserved regex chars?
          _.replace("""/""", """\/""")
          .replace("""+""", """\+""")
          .replace("""-""", """\-"""))
        .toSet
    }

    /**
      * Gets invalid format terms by reading files specified by formatStopwordFiles. Takes those
      * terms and constructs a massive regular expression. Removes those invalid terms from the
      * provided string as well as empty/duplicate whitespace. This filter is case insensitive.
      *
      */
    val stripInvalidFormats: SingleStringEnrichment = {
      // TODO Move this out of the method so the file aren't re-read for every record (see VocabEnforcer also)
      val stopWords = getFormatStopwords

      // FIXME There is a case not easily addressed here where a stop word term exists inside a larger term.
      //  Is this actually a problem? E.g. 'application/xmlphotograph' will become 'photograph'
      //  This might be more of a problem with IANA term list because it contains values like 'http', 'index',
      //  and 'widget'. Need to discuss with @gretchen

      // Create a regex from those stop words that ignores case
      // Example: (?i)(jpeg 2000)|(jpeg)|(application\/text)
      val regex = "(?i)" + stopWords
        .map(w => s"($w)")
        .mkString("|")

      // Perform the removal and trim white space
      value.replaceAll(regex, "").trim
    }
  }
}
