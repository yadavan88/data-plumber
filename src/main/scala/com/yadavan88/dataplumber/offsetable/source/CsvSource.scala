package com.yadavan88.dataplumber.offsetable.source

import cats.effect.IO

import java.io.File
import java.time.ZoneOffset
import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import scala.io.Source
import com.yadavan88.dataplumber.offsetable.DataSource
import com.yadavan88.dataplumber.offsetable.Offset
import com.yadavan88.dataplumber.offsetable.ReadResult
import cats.effect.kernel.Resource

trait CsvSource[T] extends DataSource[T] {
  def location: String

  def read(offset: Option[Offset]): IO[ReadResult[T]] = {

    val fileResource = Resource.fromAutoCloseable(IO(Source.fromFile(location)))

    fileResource.use { source =>
      IO {
        println(s"reading from csv file: $location")
        val lines = source.getLines().toList
        // Skip header line
        val dataRows = lines.tail
        val rows = dataRows.map(fromCSVRow)
        // This impl always reads the full file, so no offset is needed
        ReadResult(rows, None)
      }
    }
  }

  protected def fromCSVRow(row: String): T
}
