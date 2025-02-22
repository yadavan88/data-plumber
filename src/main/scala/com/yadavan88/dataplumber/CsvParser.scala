package com.yadavan88.dataplumber

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror

// Type class for parsing a CSV value to any type
trait CsvParser[T]:
  def parse(value: String): T

object CsvParser:
  given CsvParser[String] with
    def parse(value: String): String = value.trim

  given CsvParser[Int] with
    def parse(value: String): Int = value.trim.toInt

  given CsvParser[Long] with
    def parse(value: String): Long = value.trim.toLong

  given CsvParser[Double] with
    def parse(value: String): Double = value.trim.toDouble

  given CsvParser[LocalDate] with
    def parse(value: String): LocalDate = 
      LocalDate.parse(value.trim, DateTimeFormatter.ISO_DATE)

  given CsvParser[LocalDateTime] with
    def parse(value: String): LocalDateTime = 
      val valueTrimmed = value.trim
      if valueTrimmed.endsWith("Z") then
        LocalDateTime.parse(valueTrimmed.dropRight(1), DateTimeFormatter.ISO_DATE_TIME).atOffset(ZoneOffset.UTC).toLocalDateTime
      else
        LocalDateTime.parse(valueTrimmed, DateTimeFormatter.ISO_DATE_TIME)


  inline def summonParsers[T <: Tuple]: List[CsvParser[?]] =
    inline erasedValue[T] match
      case _: (t *: ts) => summonInline[CsvParser[t]] :: summonParsers[ts]
      case _: EmptyTuple => Nil

// Convert List[String] into a tuple dynamically
inline def tupleFromCsv[T <: Tuple](values: List[String], parsers: List[CsvParser[?]]): T =
  values.zip(parsers).map { case (v, parser) =>
    parser.asInstanceOf[CsvParser[Any]].parse(v)
  } match {
    case list => Tuple.fromArray(list.toArray).asInstanceOf[T]
  }

inline def fromCsvRow[A](row: List[String])(using m: Mirror.ProductOf[A]): A =
  val parsers = CsvParser.summonParsers[m.MirroredElemTypes]
  val tuple = tupleFromCsv[m.MirroredElemTypes](row, parsers)
  m.fromProduct(tuple)
