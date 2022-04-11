package dpla.eleanor.harvesters.opds1

import java.io.{FileWriter, PrintWriter}
import java.net.URL
import java.time.Instant

import dpla.eleanor.harvesters.Retry

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, Node, XML}

/**
  * Responsible for harvesting an ebooks feed and writing it out into a file (most likely an XML file)
  *
  * @return The tmp file location
  */
trait Opds1FeedHarvester extends Retry {
  def harvestFeed(urlStr: String): Try[String]= {
    // write xml file to /tmp location
    val timestamp = java.sql.Timestamp.from(Instant.now)
    val tmpOutput = s"${System.getProperty("java.io.tmpdir")}eleanor_$timestamp.xml"

    implicit val o: PrintWriter = new PrintWriter(new FileWriter(tmpOutput))
    try {
      Try {
        parsePages(new URL(urlStr), callback)
        tmpOutput
      }
    } finally {
      o.close()
    }
  }

  /**
    * Collapse XML and write out one record per line
    * @param elem
    * @param out
    */
  def callback(elem: Elem)(implicit out: PrintWriter): Unit =
    out.println(
      elem
        .toString()
        .trim()
        .replaceAll("\\s", " ")
    )

  @tailrec
  final def parsePages(location: URL, callback: Elem => Unit): Unit = {
    println(s"Requesting....${location.toString}")
    val xml: Try[Node] = retry(5) {
      XML.load(location)
    }

    xml match {
      case Success(root) => {

        (root \ "entry")
          .collect({ case e: Elem => callback(e) })

        val next = for {
          link <- root \ "link"
          if link \@ "rel" == "next"
        } yield link \@ "href"

        next.headOption match {
          case Some(x) => parsePages(new URL(x), callback)
          case None => Unit
        }
      }
      case Failure(_) => Unit
    }
  }
}
