package dpla.ingestion3.mappers.cdl

case class CDLDocument(
                        urlItem: Option[String] = None,
                        imageMD5: Option[String] = None,
                        titles: Seq[String] = Seq(),
                        identifiers: Seq[String] = Seq(),
                        dates: Seq[String] = Seq(),
                        rights: Seq[String] = Seq(),
                        contributors: Seq[String] = Seq(),
                        creators: Seq[String] = Seq(),
                        collectionNames: Seq[String] = Seq(),
                        publishers: Seq[String] = Seq(),
                        types: Seq[String] = Seq(),
                        campus: Option[String] = None,
                        repository: Option[String] = None,
                        provider: Option[String] = None
                      )
