package com.yadavan88.dataplumber.effectful.source

import cats.effect.IO
import com.yadavan88.dataplumber.effectful.DataSource

import java.io.File
import java.time.ZoneOffset
import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import scala.io.Source

// CSV Source
trait CsvSource[T] extends DataSource[T] {
  def location: String

  def read: IO[List[T]] = {
    IO {
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
  }

  protected def fromCSVRow(row: String): T
}
