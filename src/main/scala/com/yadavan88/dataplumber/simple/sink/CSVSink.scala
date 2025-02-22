package com.yadavan88.dataplumber.simple.sink

import com.yadavan88.dataplumber.simple.DataSink

import java.io.{File, PrintWriter}

trait CSVSink[T] extends DataSink[T] {
  def location: String
  
  def write(rows: List[T]): Unit = {
    val file = new File(location)
    val writer = new PrintWriter(file)

    val txt = rows.map(toCSVRow)
    val fullCsv = (List(headers) ++ txt).mkString("\n")
    writer.write(fullCsv)
    writer.close()
  }

  protected def headers: String
  protected def toCSVRow(value: T): String
}