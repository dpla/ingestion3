package dpla.ingestion3.enrichments.date


case class DatePattern(pattern: String, label: String)


object DatePatterns {
  val delim = "\\s*[\\/-]*\\s*"

  val regexLabelMap: List[DatePattern] = List(
    // Single date
    DatePattern(s"\\d{4}", "yyyy"), // 1942

    // Four digit year, one or two digits (month or day), one or two digits (month or day)
    DatePattern(s"^\\d{4}$delim\\d{1,2}$delim\\d{1,2}$$", "yyyy m(m) d(d)") // 1942 02 12 OR 1874-01-15 OR 1987/1/3


    // alpha month
//    s"\\d{4}$delimiter\\d{1,2}" -> "yyyy-m(m)", // 1934-01


//    s"\\d{4}$delimiter\\d{4}" -> "yyyy yyyy", // 1942/1943
//    s"\\d{4}$delimiter\\d{1,2}$delimiter\\d{1,2}$delimiter\\d{4}$delimiter\\d{1,2}$delimiter\\d{1,2}"
//      -> "yyyy m(m) d(d) yyyy m(m) d(d)", // 1850-01-01/1950-12-31

  )
}


class DatePatternMatch {

  /**
    * Finds first match
    *
    * @param date Original string value
    * @return
    */
  def identifyPattern(date: String): Option[String] = {
    DatePatterns.regexLabelMap.flatMap(datePattern => {

      if(date matches datePattern.pattern) {
        println(date + " " + datePattern.pattern)
        Some(datePattern.label)
      }
      else
        None
    }).headOption
  }

}
