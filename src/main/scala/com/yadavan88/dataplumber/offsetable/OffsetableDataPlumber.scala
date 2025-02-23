package com.yadavan88.dataplumber.offsetable

import cats.effect.IO

import java.time.LocalDateTime
import scala.util.control.NonFatal
import cats.effect.implicits.*
import cats.effect.*
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
      rows <- source.read(offset)
      transformed = transform(rows)
      newOffset <- sink.write(transformed)
      _ <- redisClient.set(OFFSET_KEY)
    } yield ()).recoverWith {
      case NonFatal(error) =>
        IO.println(s"Error occurred while running DataPlumber: $error. Performing error handling hook") >>
          handleError(error)
    }
  }

  private def setNewOffset(offset: Option[Offset]) = {

    offset.map { off =>
      for {
        _ <- redisClient.set(OFFSET_KEY, off.offset)
        _ <- redisClient.set(OFFSET_DT, off.dateTime.toString)
      } yield ()
    }.traverse(identity)
  }

  /**
   * This function transform the source datastructure into the sink datastructure.
   */
  def transform(rows: List[S]): List[D]

  /**
   * This function handle the error depending on the requirement.
   * For example, we can delete the inserted rows or log the error.
   */
  def handleError(error: Throwable): IO[Unit]

}

trait DataSource[S] {
  def read(offset: Option[Offset]): IO[List[S]]
}

trait DataSink[D] {
  def write(rows: List[D], readOffset: Option[Offset]): IO[Option[Offset]]
}

case class Offset(offset: String, dateTime: LocalDateTime)