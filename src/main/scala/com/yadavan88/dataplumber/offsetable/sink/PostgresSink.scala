package com.yadavan88.dataplumber.offsetable.sink

import com.yadavan88.dataplumber.offsetable.DataSink
import cats.effect.IO
import java.time.LocalDateTime
import com.yadavan88.dataplumber.offsetable.Offset
import doobie.util.transactor.Transactor
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*

trait PostgresSink[T: Write] extends DataSink[T] {
  def tableName: String
  def connectionString: String

  private val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    connectionString,
    "",
    "",
    None
  )

  def write(rows: List[T], readOffset: Option[Offset]): IO[Unit] = {
    if (rows.isEmpty) {
      IO.pure(None)
    } else {
      
      val valuesFragments = rows.map { row =>
        fr"(" ++ fr"$row" ++ fr")"
      }.reduce(_ ++ fr"," ++ _)

      val query =
        fr"INSERT INTO " ++ Fragment.const(tableName) ++ fr" VALUES " ++ valuesFragments

      query.update.run
      .transact(xa).void
    }
  }

}
