val string = "a 2"
val digits = 4

val outString = if (string.isEmpty) string
else if (!string.trim.forall(Character.isDigit)) string
else {
  val formatString = "%0" + digits + "d"
  println(formatString)
  formatString.format(string.trim.toInt)
}


