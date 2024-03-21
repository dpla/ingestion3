package dpla.ingestion3


package object dataStorage {

  lazy val s3Protocols: List[String] = List("s3", "s3a", "s3n")


  /**
    * Component parts of an S3 address.
    *
    * @param protocol One of ["s3", "s3n", "s3a"]
    * @param bucket   The name of the S3 bucket
    * @param prefix   Nested folder(s) beneath the bucket
    */
  case class S3Address(protocol: String,
                       bucket: String,
                       prefix: Option[String])

  object S3Address {
    // Get full S3 path. For sanity, handle leading/trailing slashes.
    def fullPath(address: S3Address): String =
      address.protocol + "://" +
        address.bucket.stripPrefix("/").stripSuffix("/") + "/" +
        address.prefix.getOrElse("").stripPrefix("/").stripSuffix("/")
  }

  /**
    * Parse an S3 address from a given String.
    *
    * @param path Path to an S3 folder
    * @return     The component parts of an S3 address
    *
    * @throws RuntimeException if unable to parse valid S3 address.
    */
  def parseS3Address(path: String): S3Address = {
    val protocol: String = path.split("://").headOption.getOrElse("")

    if (!s3Protocols.contains(protocol))
      throw new RuntimeException(s"Unable to parse S3 protocol from $path.")

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
