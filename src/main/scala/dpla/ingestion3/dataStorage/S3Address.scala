package dpla.ingestion3

package object dataStorage {

  val s3Protocols: List[String] = List("s3", "s3a", "s3n")

  val validS3Protocols: List[String] = List("s3a")

  case class S3Address(protocol: String,
                       bucket: String,
                       prefix: Option[String],
                       suffix: Option[String] = None)

  object S3Address {
    def key(address: S3Address): String =
      List(address.prefix, address.suffix).flatten.mkString("/")

    def fullPath(address: S3Address): String =
      address.protocol + "://" + address.bucket + "/" + S3Address.key(address)
  }

  /**
    *
    * @param path
    * @return
    */
  def parseS3Address(path: String): S3Address = {
    val protocol: String = path.split("://").headOption.getOrElse("")

    if (!s3Protocols.contains(protocol))
      throw new RuntimeException(s"Unable to parse S3 protocol from $path")

    val bucket: String = path.split("/").lift(2) match {
      case Some(x) => x.stripSuffix("/")
      case None =>
        throw new RuntimeException(s"Unable to parse S3 bucket from $path.")
    }

    val prefixString: String = path.stripPrefix(protocol).stripPrefix("://")
      .stripPrefix(bucket).stripPrefix("/").stripSuffix("/")

    val prefix: Option[String] =
      if (prefixString.isEmpty) None else Some(prefixString)

    S3Address(protocol=protocol, bucket=bucket, prefix=prefix)
  }
}
