import java.io.File
import java.io.PrintWriter
import scala.deriving.Mirror
import scala.compiletime.{constValueTuple, summonInline}

trait CSVSink[T <: Product] {
  def location: String

  inline def labelsOf(using p: Mirror.ProductOf[T]) =
    constValueTuple[p.MirroredElemLabels]

  inline def write(rows: List[T])(using m: Mirror.ProductOf[T]): Unit = {
    val file = new File(location)
    val writer = new PrintWriter(file)
    // works, but it is a hack!
    val headers = labelsOf.toString.replaceAll("\\(", "").replaceAll("\\)", "")
    println("headers:: " + headers)

    val txt = rows.map(row => row.productIterator.mkString(","))
    val fullCsv = (headers :: txt).mkString("\n")
    writer.write(fullCsv)
    writer.close()
  }
}
