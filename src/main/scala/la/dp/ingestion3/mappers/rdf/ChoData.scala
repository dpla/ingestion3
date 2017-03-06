package la.dp.ingestion3.mappers.rdf

import org.eclipse.rdf4j.model.{Literal, Resource}

case class ChoData(
                dates: Seq[Resource],
                titles: Seq[Literal],
                identifiers: Seq[Literal],
                rights: Seq[Literal],
                collections: Seq[Literal],
                contributors: Seq[Literal],
                creators: Seq[Literal],
                publishers: Seq[Literal],
                types: Seq[Literal]
              )
