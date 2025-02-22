import scala.deriving.Mirror
import scala.compiletime.{erasedValue, summonInline}
import java.time.ZoneOffset
import java.io.File
import scala.io.Source

// CSV Source
trait CsvSourceSimple[T] extends DataSource[T] {
  def location: String

  def read: List[T] = {
    println(s"reading from csv file: $location")
    val source = Source.fromFile(location)
    try {
      val lines = source.getLines().toList
      // Skip header line
      val dataRows = lines.tail
      dataRows.map(fromCSVRow)
    } finally {
      source.close()
    }
  }

  protected def fromCSVRow(row: String): T
}
