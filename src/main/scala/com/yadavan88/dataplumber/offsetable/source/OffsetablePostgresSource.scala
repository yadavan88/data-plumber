package com.yadavan88.dataplumber.offsetable.source

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import com.yadavan88.dataplumber.offsetable.Offset
import com.yadavan88.dataplumber.offsetable.DataSource
import com.yadavan88.dataplumber.offsetable.ReadResult
import com.yadavan88.dataplumber.offsetable.Offsetable
import java.time.LocalDateTime

trait OffsetablePostgresSource[T <: Offsetable: Read] extends DataSource[T] {
  def tableName: String
  def connectionString: String

  private val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    connectionString,
    "username",
    "password",
    None
  )

  def read(lastOffset: Option[Offset]): IO[ReadResult[T]] = {

    val offsetFilter = lastOffset
      .map { offset =>
        fr" WHERE id > CAST(" ++ Fragment.const(offset.lastOffset) ++ fr" AS BIGINT)"
      }
      .getOrElse(Fragment.empty)

    val query = fr"SELECT * FROM" ++ Fragment.const(
      tableName
    ) ++ offsetFilter ++ fr" ORDER BY id"
    val rowsIO = query
      .query[T]
      .to[List]
      .transact(xa)

    rowsIO.map { rows =>
      val nextOffset = rows
        .sortBy(_.id)
        .lastOption
        .map(o => Offset(o.id.toString, LocalDateTime.now().toString))
      ReadResult(rows, nextOffset)
    }
  }
}
