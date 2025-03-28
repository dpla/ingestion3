package dpla.ingestion3.harvesters.oai

/** Trait that specifies OAI calls for the common functionality the data source
  * needs.
  */

trait OaiMethods {

  def listAllRecordPagesForSet(
      setSpec: String
  ): IterableOnce[OaiPage]

  def parsePageIntoRecords(
      page: OaiPage,
      removeDeleted: Boolean
  ): IterableOnce[OaiRecord]

  def listAllSetPages(): IterableOnce[OaiPage]

  def parsePageIntoSets(
      page: OaiPage
  ): IterableOnce[OaiSet]

  def listAllRecordPages(): IterableOnce[OaiPage]
}
