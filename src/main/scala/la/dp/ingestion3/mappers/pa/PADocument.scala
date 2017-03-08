package la.dp.ingestion3.mappers.pa


case class PADocument (
                        contributors: Seq[String] = Seq(),
                        coverages: Seq[String] = Seq(),
                        creators: Seq[String] = Seq(),
                        dates: Seq[String] = Seq(),
                        description: Option[String] = None,
                        formats: Seq[String] = Seq(),
                        identifiers: Seq[String] = Seq(),
                        languages: Seq[String] = Seq(),
                        publishers: Seq[String] = Seq(),
                        relations: Seq[String] = Seq(),
                        rights: Seq[String] = Seq(),
                        subjects: Seq[String] = Seq(),
                        source: Option[String] = None,
                        titles: Seq[String] = Seq(),
                        types: Seq[String] = Seq()
                      )
