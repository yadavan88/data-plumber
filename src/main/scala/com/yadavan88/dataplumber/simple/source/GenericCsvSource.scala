package com.yadavan88.dataplumber.simple.source

import com.yadavan88.dataplumber.GenericCSVParser.fromCsvRow

import java.io.File
import java.time.ZoneOffset
import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import scala.io.Source

// CSV Source
trait GenericCsvSource[T <: Product] {
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
