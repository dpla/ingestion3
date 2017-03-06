package la.dp.ingestion3.mappers.rdf

trait DefaultVocabularies {
  val cnt = CNT()
  val dc = DC()
  val dcmiType = DCMIType()
  val dcTerms = DCTerms()
  val dpla = DPLA()
  val edm = EDM()
  val gn = GN()
  val oa = OA()
  val ore = ORE()
  val rdf = RDF()
  val skos = SKOS()
  val wgs84 = WGS84()

  def defaultVocabularies: Seq[Vocabulary] = Seq(
    cnt, dc, dcmiType, dcTerms, dpla, edm, gn, oa, ore, rdf, skos, wgs84
  )
}