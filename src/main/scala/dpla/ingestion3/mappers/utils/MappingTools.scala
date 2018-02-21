package dpla.ingestion3.mappers.utils

import dpla.ingestion3.profiles.PaProfile
import dpla.ingestion3.utils.Utils

import scala.util.Success

case class Document[T](value: T) {
  def get: T = value
}




// the main() method for running a mapping
object MappingExecutor extends App {
  val profile: PaProfile = new PaProfile()

  val xml =
    <record>
      <header>
        <identifier>oai:libcollab.temple.edu:dplapa:BLOOMS_blmmap_2</identifier>
        <datestamp>2017-09-11T20:47:12Z</datestamp>
      </header>
      <metadata>
        <oai_dc:dc xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
          <dc:title>Draught of a tract of land, situate in Bloom Township, Columbia County</dc:title>
          <dc:subject>Bloom (Columbia County, Pa : Township) - Maps</dc:subject>
          <dc:subject>Paxton, Joseph, 1786-1861</dc:subject>
          <dc:subject>Hughes, George, 1798-1881</dc:subject>
          <dc:subject>Ramsey, Doctor</dc:subject>
          <dc:subject>Boone, Samuel</dc:subject>
          <dc:subject>Knorr, Thomas, 1803-1877</dc:subject>
          <dc:description>Survey map of a draught of a tract of land, situate in Bloom Township, Columbia County, belonging to the estate of Joseph Paxton, decd. containing 105 acres, and 62 perches neat measured, including the r. road and canal in the computation. Resurveyed January 13th A.D. 1862 by Lewis Yetter, in r. road 313 perches In canal 373 perches. Farm in Bloom Township.</dc:description>
          <dc:contributor>Yetter, Lewis, 1811-1880</dc:contributor>
          <dc:contributor>Bloomsburg University</dc:contributor>
          <dc:date>1862</dc:date><dc:date>1862</dc:date>
          <dc:type>Map</dc:type
          ><dc:type>Image</dc:type>
          <dc:format>image/jpeg</dc:format>
          <dc:identifier>dplapa:BLOOMS_blmmap_2</dc:identifier>
          <dc:identifier>http://cdm17189.contentdm.oclc.org/cdm/ref/collection/blmmap/id/2</dc:identifier>
          <dc:identifier>http://cdm17189.contentdm.oclc.org/utils/getthumbnail/collection/blmmap/id/2</dc:identifier>
          <dc:source>Keystone Library Network</dc:source>
          <dc:relation>Bloomsburg University Map Collection</dc:relation>
          <dc:coverage>United States - Pennsylvania - Columbia County - Bloom Township</dc:coverage>
          <dc:rights>Andruss Library digital images and corresponding text may be used for non-commercial, educational, and personal use only without permission, provided that proper attribution of the source accompanies the image. The digital images are not intended for reproduction or redistribution. Commercial publication requires permission. Contact the Bloomsburg University Archives at guides.library.bloomu.edu/universityarchives or (570) 389-4210. Andruss Library assumes no responsibility for infringement of copyright by content users.</dc:rights>
        </oai_dc:dc>
      </metadata>
    </record>

  val mappedRecord = profile.performMapping(xml.toString())

  mappedRecord match {
    case Success(r) => println(Utils.formatOreAggregation(r))
  }
}


