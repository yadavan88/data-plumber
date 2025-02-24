package com.yadavan88.dataplumber.offsetable

import cats.effect.IO

import java.time.LocalDateTime
import scala.util.control.NonFatal
import cats.effect.implicits.*
import cats.syntax.traverse.*

trait OffsetableDataPlumber[S, D] {
  def source: DataSource[S]

  def sink: DataSink[D]

  def name: String

  def redisHost: String

  lazy val redisClient = new RedisClient(redisHost, name)

  private val OFFSET_KEY = "offset"
  private val OFFSET_DT = "offset-dt"

  final def run = {
    (for {
      offsetValue <- redisClient.get(OFFSET_KEY)
      offsetDT <- redisClient.get(OFFSET_DT)
      offset = offsetValue.flatMap(off => offsetDT.map(Offset(off, _)))
      readResult <- source.read(offset)
      transformed = transform(readResult)
      newOffset <- sink.write(transformed, offset)
      _ <- setNewOffset(readResult.nextOffset)
      _ <- IO.println(s"**** Successfully processed ${readResult.rows.size} rows ****")
    } yield ()).recoverWith {
      case NonFatal(error) =>
        IO.println(s"Error occurred while running DataPlumber: $error. Performing error handling hook") >>
          handleError(error)
    }
  }

  private def setNewOffset(offset: Option[Offset]) = {

    offset.map { off =>
      for {
        _ <- redisClient.set(OFFSET_KEY, off.lastOffset)
        _ <- redisClient.set(OFFSET_DT, off.dateTime.toString)
      } yield ()
    }.traverse(identity)
  }

  /**
   * This function transform the source datastructure into the sink datastructure.
   */
  def transform(readResult: ReadResult[S]): List[D]

  /**
   * This function handle the error depending on the requirement.
   * For example, we can delete the inserted rows or log the error.
   */
  def handleError(error: Throwable): IO[Unit]

}

trait DataSource[S] {
  def read(offset: Option[Offset]): IO[ReadResult[S]]
}

trait DataSink[D] {
  def write(rows: List[D], lastOffset: Option[Offset]): IO[Unit]
}

case class Offset(lastOffset: String, dateTime: String)

case class ReadResult[T](rows: List[T], nextOffset: Option[Offset])

trait Offsetable {
  def id: Long
}