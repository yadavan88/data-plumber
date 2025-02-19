import scala.deriving.Mirror
import scala.compiletime.{erasedValue, summonInline}
import java.time.ZoneOffset
import java.io.File
import scala.io.Source

// CSV Source
trait CsvSource[T] {
  def location: String

  inline def read(using m: Mirror.ProductOf[T]): List[T] = {
    val file = new File(location)
    val rows = Source
      .fromFile(file)
      .getLines()
      .map(_.split(",").map(_.trim).toList)
      .toList

    rows.tail.map(row => fromCsvRow[T](row))
  }
}
