package la.dp.ingestion3.data

import scala.xml.Elem

/**
  * Created by scott on 1/22/17.
  */
object TestOaiData {

  def getPaOai(): Elem = { pa_oai }
  def getPaOaiError: Elem = { pa_error }
  /**
    * Local data for testing...
    */

    val pa_error: Elem =
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
      <responseDate>2017-01-27T05:10:17Z</responseDate>
      <request verb="ListRecords">http://localhost:8080/fedora/oai</request>
      <error code="cannotDisseminateFormat">
        Repository does not provide that format in OAI-PMH responses.
      </error>
    </OAI-PMH>

  val pa_oai: Elem =
    <OAI-PMH
    xmlns="http://www.openarchives.org/OAI/2.0/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
      <responseDate>2017-01-21T17:24:56Z</responseDate>
      <request metadataPrefix="oai_dc" verb="ListRecords">http://localhost:8080/fedora/oai</request>
      <ListRecords>
        <record>
          <header>
            <identifier>
              oai:libcollab.temple.edu:fedora-system:ContentModel-3.0
            </identifier>
            <datestamp>2008-07-02T05:09:44Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>Content Model Object for Content Model Objects</dc:title>
              <dc:identifier>fedora-system:ContentModel-3.0</dc:identifier>
            </oai_dc:dc>
          </metadata>
        </record>
        <record>
          <header>
            <identifier>
              oai:libcollab.temple.edu:fedora-system:FedoraObject-3.0
            </identifier>
            <datestamp>2008-07-02T05:09:44Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>Content Model Object for All Objects</dc:title>
              <dc:identifier>fedora-system:FedoraObject-3.0</dc:identifier>
            </oai_dc:dc>
          </metadata>
        </record>
        <record>
          <header>
            <identifier>
              oai:libcollab.temple.edu:fedora-system:ServiceDefinition-3.0
            </identifier>
            <datestamp>2008-07-02T05:09:44Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>
                Content Model Object for Service Definition Objects
              </dc:title>
              <dc:identifier>fedora-system:ServiceDefinition-3.0</dc:identifier>
            </oai_dc:dc>
          </metadata>
        </record>
        <record>
          <header>
            <identifier>
              oai:libcollab.temple.edu:fedora-system:ServiceDeployment-3.0
            </identifier>
            <datestamp>2008-07-02T05:09:44Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>
                Content Model Object for Service Deployment Objects
              </dc:title>
              <dc:identifier>fedora-system:ServiceDeployment-3.0</dc:identifier>
            </oai_dc:dc>
          </metadata>
        </record>
        <record>
          <header>
            <identifier>
              oai:libcollab.temple.edu:dplapa:ALBRIGHT_cpaphoto_43
            </identifier>
            <datestamp>2016-12-21T15:54:10Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>Boarding room</dc:title>
              <dc:creator>Charles H. Venus, 1899</dc:creator>
              <dc:subject>Dormitories</dc:subject>
              <dc:description>
                Left - Charles Adolphus Mock, Class of 1898. Right - Charles Henry Venus, Class of 1899.
              </dc:description>
              <dc:contributor>Albright College</dc:contributor>
              <dc:date>1897</dc:date>
              <dc:type>Image</dc:type>
              <dc:format>image/jpeg</dc:format>
              <dc:identifier>dplapa:ALBRIGHT_cpaphoto_43</dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/cdm/ref/collection/cpaphoto/id/43
              </dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/utils/getthumbnail/collection/cpaphoto/id/43
              </dc:identifier>
              <dc:source>POWER Library as sponsor and HSLC as maintainer</dc:source>
              <dc:relation>
                Albright College - Central Pennsylvania College Photo Collection
              </dc:relation>
              <dc:coverage>New Berlin, Union County, Pennsylvania</dc:coverage>
              <dc:rights>
                U.S. and international copyright laws protect this digital image. Commercial use or distribution of the image is not permitted without prior permission of the copyright holder. Please contact the Albright College, Special Collections for permission to use the digital image.
              </dc:rights>
            </oai_dc:dc>
          </metadata>
        </record>
        <record>
          <header>
            <identifier>
              oai:libcollab.temple.edu:dplapa:ALBRIGHT_cpaphoto_2
            </identifier>
            <datestamp>2016-12-21T15:54:12Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>Bookkeeping Class</dc:title>
              <dc:subject>Bookkeeping, Classrooms</dc:subject>
              <dc:description>
                Bookeeping class. George Vellerchamp is possibly the fourth from left.
              </dc:description>
              <dc:contributor>Albright College</dc:contributor>
              <dc:type>Image</dc:type>
              <dc:format>image/jpeg</dc:format>
              <dc:identifier>dplapa:ALBRIGHT_cpaphoto_2</dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/cdm/ref/collection/cpaphoto/id/2
              </dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/utils/getthumbnail/collection/cpaphoto/id/2
              </dc:identifier>
              <dc:source>POWER Library as sponsor and HSLC as maintainer</dc:source>
              <dc:relation>
                Albright College - Central Pennsylvania College Photo Collection
              </dc:relation>
              <dc:coverage>New Berlin, Union County, Pennsylvania</dc:coverage>
              <dc:rights>
                U.S. and international copyright laws protect this digital image. Commercial use or distribution of the image is not permitted without prior permission of the copyright holder. Please contact the Albright College, Special Collections for permission to use the digital image.
              </dc:rights>
            </oai_dc:dc>
          </metadata>
        </record>
        <record>
          <header>
            <identifier>
              oai:libcollab.temple.edu:dplapa:ALBRIGHT_cpaphoto_58
            </identifier>
            <datestamp>2016-12-21T15:54:14Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>Rollin Wilson</dc:title>
              <dc:subject>Students, Rollin Wilson</dc:subject>
              <dc:description>Rollin Eugene Wilson, Class of 1902</dc:description>
              <dc:contributor>Albright College</dc:contributor>
              <dc:date>Between 1899-1902</dc:date>
              <dc:type>Image</dc:type>
              <dc:format>image/jpeg</dc:format>
              <dc:identifier>dplapa:ALBRIGHT_cpaphoto_58</dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/cdm/ref/collection/cpaphoto/id/58
              </dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/utils/getthumbnail/collection/cpaphoto/id/58
              </dc:identifier>
              <dc:source>POWER Library as sponsor and HSLC as maintainer</dc:source>
              <dc:relation>
                Albright College - Central Pennsylvania College Photo Collection
              </dc:relation>
              <dc:coverage>New Berlin, Union County, Pennsylvania</dc:coverage>
              <dc:rights>
                U.S. and international copyright laws protect this digital image. Commercial use or distribution of the image is not permitted without prior permission of the copyright holder. Please contact the Albright College, Special Collections for permission to use the digital image.
              </dc:rights>
            </oai_dc:dc>
          </metadata>
        </record>
        <record>
          <header>
            <identifier>
              oai:libcollab.temple.edu:dplapa:ALBRIGHT_cpaphoto_40
            </identifier>
            <datestamp>2016-12-21T15:55:24Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>Lover's Log</dc:title>
              <dc:subject>Logs, Buildings</dc:subject>
              <dc:description>Lover's log. Main building is in the background.</dc:description>
              <dc:contributor>Albright College</dc:contributor>
              <dc:date>Between 1887-1902</dc:date>
              <dc:type>Image</dc:type>
              <dc:format>image/jpeg</dc:format>
              <dc:identifier>dplapa:ALBRIGHT_cpaphoto_40</dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/cdm/ref/collection/cpaphoto/id/40
              </dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/utils/getthumbnail/collection/cpaphoto/id/40
              </dc:identifier>
              <dc:source>POWER Library as sponsor and HSLC as maintainer</dc:source>
              <dc:relation>
                Albright College - Central Pennsylvania College Photo Collection
              </dc:relation>
              <dc:coverage>New Berlin, Union County, Pennsylvania</dc:coverage>
              <dc:rights>
                U.S. and international copyright laws protect this digital image. Commercial use or distribution of the image is not permitted without prior permission of the copyright holder. Please contact the Albright College, Special Collections for permission to use the digital image.
              </dc:rights>
            </oai_dc:dc>
          </metadata>
        </record>
        <record>
          <header>
            <identifier>
              oai:libcollab.temple.edu:dplapa:ALBRIGHT_churchslide_35
            </identifier>
            <datestamp>2016-12-21T15:55:54Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>Portrait of Jacob Albright</dc:title>
              <dc:creator>Fred W. Solver (1869-1951)</dc:creator>
              <dc:subject>Jacob Albright</dc:subject>
              <dc:description>
                The lantern slides were produced by Rev. Fred W. Solver that he used for a lecture about individuals and places associated with the Evangelical Church.
              </dc:description>
              <dc:contributor>Albright College</dc:contributor>
              <dc:date>Ca. 1930</dc:date>
              <dc:type>Image</dc:type>
              <dc:format>image/jpeg</dc:format>
              <dc:identifier>dplapa:ALBRIGHT_churchslide_35</dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/cdm/ref/collection/churchslide/id/35
              </dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/utils/getthumbnail/collection/churchslide/id/35
              </dc:identifier>
              <dc:source>POWER Library as sponsor and HSLC as maintainer</dc:source>
              <dc:relation>
                Albright Evangelical Church Lantern Slide Collection
              </dc:relation>
              <dc:coverage>Pennsylvania, Ohio</dc:coverage>
              <dc:rights>
                U.S. and international copyright laws protect this digital image. Commercial use or distribution of the image is not permitted without prior permission of the copyright holder. Please contact the Albright College, Special Collections for permission to use the digital image.
              </dc:rights>
            </oai_dc:dc>
          </metadata>
        </record>
        <record>
          <header>
            <identifier>
              oai:libcollab.temple.edu:dplapa:ALBRIGHT_churchslide_1
            </identifier>
            <datestamp>2016-12-21T15:55:55Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc
            xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>General conference</dc:title>
              <dc:creator>Fred W. Solver (1869-1951)</dc:creator>
              <dc:subject>Evangelical Church conference</dc:subject>
              <dc:description>
                Site of the General Conference in 1830. - The lantern slides were produced by Rev. Fred W. Solver that he used for a lecture about individuals and places associated with the Evangelical Church.
              </dc:description>
              <dc:contributor>Albright College</dc:contributor>
              <dc:date>Ca. 1930</dc:date>
              <dc:type>Image</dc:type>
              <dc:format>image/jpeg</dc:format>
              <dc:identifier>dplapa:ALBRIGHT_churchslide_1</dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/cdm/ref/collection/churchslide/id/1
              </dc:identifier>
              <dc:identifier>
                http://digitalcollections.powerlibrary.org/utils/getthumbnail/collection/churchslide/id/1
              </dc:identifier>
              <dc:source>POWER Library as sponsor and HSLC as maintainer</dc:source>
              <dc:relation>
                Albright Evangelical Church Lantern Slide Collection
              </dc:relation>
              <dc:coverage>Pennsylvania, Ohio</dc:coverage>
              <dc:rights>
                U.S. and international copyright laws protect this digital image. Commercial use or distribution of the image is not permitted without prior permission of the copyright holder. Please contact the Albright College, Special Collections for permission to use the digital image.
              </dc:rights>
            </oai_dc:dc>
          </metadata>
        </record>
        <resumptionToken expirationDate="2017-01-21T17:33:16Z" cursor="0">90d421891feba6922f57a59868d7bcd1</resumptionToken>
      </ListRecords>
    </OAI-PMH>
}
