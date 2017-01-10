package dp.la.ingestion3

import org.scalatest.{FlatSpec, Matchers}

class HelloWorld$Test extends FlatSpec with Matchers {

  "A HelloWorld" should "print 'hello, world'" in {
    HelloWorld.main(Array[String]())
  }

}
