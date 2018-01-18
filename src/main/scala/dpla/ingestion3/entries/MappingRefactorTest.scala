package dpla.ingestion3.entries

import dpla.ingestion3.mappers.providers.PaExtractor

object MappingRefactorTest {

  def main(args: Array[String]): Unit = {
    val xml =
      <record>
        <header>
          <identifier>oai:libcollab.temple.edu:dplapa:BLOOMS_blmmap_17</identifier>
          <datestamp>2017-09-11T20:47:08Z</datestamp>
        </header>
        <metadata>
          <oai_dc:dc xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
            <dc:title>Embuscade = [Ambuscade, August 31, 1778]</dc:title>
            <dc:subject>United States - History - Revolution, 1775-1783 - Maps, Manuscript</dc:subject>
            <dc:subject>United States - History - Revolution, 1775-1783 - Campaigns</dc:subject>
            <dc:subject>Indian Bridge, Battle of, Indian Bridge, N.Y., 1779 - Maps, Manuscript</dc:subject>
            <dc:contributor>Bloomsburg University</dc:contributor>
            <dc:date>1791</dc:date>
            <dc:date>1791</dc:date>
            <dc:type>Map</dc:type>
            <dc:type>Image</dc:type>
            <dc:format>image/jpeg</dc:format>
            <dc:identifier>dplapa:BLOOMS_blmmap_17</dc:identifier>
            <dc:identifier>http://cdm17189.contentdm.oclc.org/cdm/ref/collection/blmmap/id/17</dc:identifier>
            <dc:identifier>http://cdm17189.contentdm.oclc.org/utils/getthumbnail/collection/blmmap/id/17</dc:identifier>
            <dc:source>Keystone Library Network</dc:source>
            <dc:relation>Bloomsburg University Map Collection</dc:relation>
            <dc:coverage>United States - New York - Indian Bridge</dc:coverage>
            <dc:rights>Andruss Library digital images and corresponding text may be used for non-commercial, educational, and personal use only without permission, provided that proper attribution of the source accompanies the image. The digital images are not intended for reproduction or redistribution. Commercial publication requires permission. Contact the Bloomsburg University Archives at guides.library.bloomu.edu/universityarchives or (570) 389-4210. Andruss Library assumes no responsibility for infringement of copyright by content users.</dc:rights>
          </oai_dc:dc>
        </metadata>
      </record>

    // val xml = XmlExtractionUtils.parse(record)

    val extractor = new PaExtractor("pa")

    println(extractor.collection(xml))

    println(extractor.contributor(xml))

    println(extractor.`type`(xml))

    println(extractor.date(xml))

  }

}
