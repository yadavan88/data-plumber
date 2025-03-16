package com.yadavan88.dataplumber.effectful.sink

import cats.effect.IO
import com.yadavan88.dataplumber.effectful.IODataSink

import java.io.{File, PrintWriter}

trait CSVSink[T] extends IODataSink[T] {
  def location: String
  
  def write(rows: List[T]): IO[Unit] = {
    IO {
      val file = new File(location)
      val writer = new PrintWriter(file)

      val txt = rows.map(toCSVRow)
      val fullCsv = (List(headers) ++ txt).mkString("\n")
      writer.write(fullCsv)
      writer.close()
    }
  }

  protected def headers: String
  protected def toCSVRow(value: T): String

}
