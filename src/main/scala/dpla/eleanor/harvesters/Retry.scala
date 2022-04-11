package dpla.eleanor.harvesters

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

trait Retry {
  @tailrec
  final def retry[T](n: Int)(fn: => T): Try[T] =
    util.Try {
      fn
    } match {
      case Success(x) => Success(x)
      case _ if n > 1 =>
        Thread.sleep(1000)
        retry(n - 1)(fn)
      case Failure(e) => Failure(e)
    }
}
