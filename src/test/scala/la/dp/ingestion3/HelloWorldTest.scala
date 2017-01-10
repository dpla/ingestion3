package la.dp.ingestion3

import org.scalatest.{FlatSpec, Matchers}

class HelloWorldTest extends FlatSpec with Matchers {

  "A HelloWorld" should "print 'hello, world'" in {
    HelloWorld.main(Array[String]())
  }

}
