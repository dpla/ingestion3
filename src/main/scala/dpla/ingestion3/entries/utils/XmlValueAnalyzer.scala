package dpla.ingestion3.entries.utils

import dpla.ingestion3.dataStorage.InputHelper
import dpla.ingestion3.entries.Entry
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.w3c.dom._

import java.io.ByteArrayInputStream
import java.util
import javax.xml.XMLConstants
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.{XPathConstants, XPathFactory}

object XmlValueAnalyzer {

  def extractValues(xmlString: String, xpathString: String): Seq[String] = {
    val builderFactory = DocumentBuilderFactory.newDefaultInstance()
    builderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)
    builderFactory.setNamespaceAware(false)
    val builder = builderFactory.newDocumentBuilder
    val bais = new ByteArrayInputStream(
      xmlString.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    )
    val xmlDocument = builder.parse(bais)
    bais.close()
    val xPathFactory = XPathFactory.newInstance()
    xPathFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)
    val xPath = xPathFactory.newXPath

    xPath.setNamespaceContext(new javax.xml.namespace.NamespaceContext {
      def getNamespaceURI(prefix: String): String = ""
      def getPrefix(uri: String): String = ""
      def getPrefixes(uri: String): util.Iterator[String] =
        java.util.Collections.emptyIterator[String]
    })

    val nodeList =
      xPath
        .compile(xpathString)
        .evaluate(xmlDocument, XPathConstants.NODESET)
        .asInstanceOf[org.w3c.dom.NodeList]

    for (i <- 0 until nodeList.getLength) yield {
      val node = nodeList.item(i)
      if (node != null) {
        node match {
          case element: Element => element.getTextContent
          case attribute: Attr  => attribute.getValue
          case entity: Entity   => entity.getTextContent
          case entityReference: EntityReference =>
            entityReference.getTextContent
          case documentFragment: DocumentFragment =>
            documentFragment.getTextContent

          case text: Text          => text.getTextContent
          case comment: Comment    => comment.getTextContent
          case processingInstruction: ProcessingInstruction =>
            processingInstruction.getTextContent

          case document: Document => document.getDocumentElement.getTextContent
          case documentType: DocumentType => documentType.getName
          case notation: Notation => notation.getPublicId
          case _ => "" // unhandled
        }
      } else ""
    }

  }

  /** Spark UDF that takes an XML string and returns a list of values for an
    * xpath
    */
  private val extractValuesUdf =
    udf((xmlString: String, xpathString: String) =>
      extractValues(xmlString, xpathString)
    )

  /** Analyzes a DataFrame containing XML strings and returns a DataFrame with
    * XPath counts
    * @param df
    *   Input DataFrame containing XML strings in the specified column
    * @param xmlColumn
    *   Name of the column containing XML strings
    * @return
    *   DataFrame with columns: xpath (String) and count (Long)
    */
  def analyzeXmlValues(
      df: DataFrame,
      xmlColumn: String,
      xpathString: String
  ): DataFrame =
    df.select(
      explode(extractValuesUdf(col(xmlColumn), lit(xpathString))).alias("value")
    ).groupBy("value")
      .agg(count("*").alias("count"))
      .orderBy(col("value").desc)

//  def main(args: Array[String]): Unit = {
//    val document = """<record>
//                     |      <header xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
//                     |        <identifier>10362236</identifier>
//                     |        <datestamp>2024-04-23T17:32:03Z</datestamp>
//                     |      </header>
//                     |      <metadata xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
//                     |        <oai_dc:dc xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
//                     |          <oai_dc:title>Student Mobilization Committee To End the War In Vietnam -- Bring The GIs Home Now! </oai_dc:title>
//                     |          <oai_dc:creator>Unknown</oai_dc:creator>
//                     |          <oai_dc:subject>Vietnam War, 1961-1975; anti-war demonstrations; protest movements; peace movements; pacifism; student movements; crowds; students; youth and war; strikes; universities &amp; colleges; New York (N.Y.)</oai_dc:subject>
//                     |          <oai_dc:description>The Student Mobilization Committee to End the War in Vietnam was a student organization opposed to the United States' war in Vietnam. This sticker is publicizing a student strike and demonstration held in New York City and features a photographic image of a very large gathering of protesters demonstrating against the war.  Additional text includes: Student Strike Nov. 3 -- March Against The War Nov. 6 -- Labor Donated -- 150 Fifth Avenue, Rm. 911 / N.Y.C. 10011 -- Telephone (212) 741-1960</oai_dc:description>
//                     |          <oai_dc:date>1969</oai_dc:date>
//                     |          <oai_dc:type>Political sticker</oai_dc:type>
//                     |          <oai_dc:identifier>sag_us_historical_2012_ebayaug_001_se.tif</oai_dc:identifier>
//                     |          <oai_dc:identifier>Thumbnail:https://forum.jstor.org/oai/thumbnail?id=10362236</oai_dc:identifier>
//                     |          <oai_dc:identifier>Medias:https://forum.jstor.org/oai/medias/10362236</oai_dc:identifier>
//                     |          <oai_dc:identifier>FullSize:https://forum.jstor.org/assets/10362236/representation/size/9</oai_dc:identifier>
//                     |          <oai_dc:identifier>ADLImageViewer:https://www.jstor.org/stable/10.2307/community.10362236</oai_dc:identifier>
//                     |          <oai_dc:identifier>SSCImageViewer:https://www.jstor.org/stable/10.2307/community.10362236</oai_dc:identifier>
//                     |          <oai_dc:source>Student Mobilization Committee to End the War in Vietnam</oai_dc:source>
//                     |          <oai_dc:language>English</oai_dc:language>
//                     |          <oai_dc:coverage>United States</oai_dc:coverage>
//                     |          <oai_dc:rights>The Andrew W. Mellon Foundation and the Council of Independent Colleges provided funding support for this project.</oai_dc:rights>
//                     |        </oai_dc:dc>
//                     |      </metadata>
//                     |      <about>
//                     |        <request verb="ListRecords" metadataPrefix="oai_dc" set="1041"/>
//                     |        <responseDate>2025-04-09T17:21:25.947Z</responseDate>
//                     |      </about>
//                     |    </record>""".stripMargin
//    val results = extractValues(document, "/record/metadata/dc/identifier/text()[contains(., ':')]/substring-before(., ':')")
//    results.foreach(println)
//  }

  /** Main function to demonstrate usage
    */
  def main(args: Array[String]): Unit = {

    Entry.suppressUnsafeWarnings()

    val input = args(0)
    val xpathString = args(1)
    val output = args(2)

    val harvestData =
      if (InputHelper.isActivityPath(input)) input
      else
        InputHelper
          .mostRecent(input)
          .getOrElse(throw new RuntimeException("Unable to load harvest data."))

    val spark = SparkSession
      .builder()
      .master("local[6]")
      .appName("XML Value Analyzer")
      .getOrCreate()

    try {
      val df = spark.read.format("avro").load(harvestData)
      val results = analyzeXmlValues(df, "document", xpathString)
      results.coalesce(1).write.csv(output)

    } finally {
      spark.stop()
    }
  }
}
