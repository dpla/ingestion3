package dpla.ingestion3.mappers.rdf

import org.eclipse.rdf4j.model.{Literal, Resource}

case class ChoData(
                dates: Seq[Resource] = Seq(),
                titles: Seq[Literal] = Seq(),
                identifiers: Seq[Literal] = Seq(),
                rights: Seq[Literal] = Seq(),
                collections: Seq[Literal] = Seq(),
                contributors: Seq[Literal] = Seq(),
                creators: Seq[Literal] = Seq(),
                publishers: Seq[Literal] = Seq(),
                types: Seq[Literal] = Seq()
              )
