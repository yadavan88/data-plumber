package com.yadavan88.dataplumber.streaming.source

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import fs2.*
import java.time.LocalDateTime
import _root_.com.yadavan88.dataplumber.streaming.StreamingDataSource
import _root_.com.yadavan88.dataplumber.offsetable.Offsetable
import _root_.com.yadavan88.dataplumber.offsetable.Offset

trait StreamingPostgresSource[T <: Offsetable: Read] extends StreamingDataSource[T] {
  def tableName: String
  def connectionString: String

  private val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    connectionString,
    "username",
    "password",
    None
  )

  def read(lastOffset: Option[Offset]): Stream[IO, T] = {
    val offsetFilter = lastOffset
      .map { offset =>
        fr" WHERE id > ${offset.lastOffset}"
      }
      .getOrElse(Fragment.empty)

    val query = fr"SELECT * FROM" ++ Fragment.const(
      tableName
    ) ++ offsetFilter ++ fr" ORDER BY id"

    query
      .query[T]
      .stream
      .transact(xa)
  }
}
