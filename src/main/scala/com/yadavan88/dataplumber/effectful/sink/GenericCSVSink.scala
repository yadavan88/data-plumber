package com.yadavan88.dataplumber.effectful.sink

import cats.effect.IO

import java.io.{File, PrintWriter}
import scala.compiletime.{constValueTuple, summonInline}
import scala.deriving.Mirror

trait GenericCSVSink[T <: Product] {
  def location: String

  inline def labelsOf(using m: Mirror.ProductOf[T]) = {
    constValueTuple[m.MirroredElemLabels]
  }

  inline def write(rows: List[T])(using m: Mirror.ProductOf[T]): IO[Unit]= {
    IO {
    val file = new File(location)
    val writer = new PrintWriter(file)
    // works, but it is a hack!
    val headers = labelsOf.toString.replaceAll("\\(", "").replaceAll("\\)", "")

    val txt = rows.map(row => row.productIterator.mkString(","))
    val fullCsv = (headers :: txt).mkString("\n")
    writer.write(fullCsv)
    writer.close()
    }
  }
}
