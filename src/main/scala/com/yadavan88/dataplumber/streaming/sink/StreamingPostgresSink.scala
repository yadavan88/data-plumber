package com.yadavan88.dataplumber.streaming.sink

import com.yadavan88.dataplumber.offsetable.DataSink
import cats.effect.IO
import java.time.LocalDateTime
import com.yadavan88.dataplumber.offsetable.Offset
import doobie.util.transactor.Transactor
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*
import com.yadavan88.dataplumber.streaming.StreamingDataSink
import fs2.*
import doobie.util.fragment.Fragment
import doobie.util.update.Update0
import com.yadavan88.dataplumber.offsetable.Offsetable

trait StreamingPostgresSink[T: Write] extends StreamingDataSink[T] {
  def tableName: String
  def connectionString: String
  def columnNames: List[String] 

  private val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    connectionString,
    "",
    "",
    None
  )

  def write: Pipe[IO, Chunk[T], Chunk[T]] = { stream =>
    stream.evalMap { chunk =>
      if (chunk.isEmpty) {
        IO.pure(Chunk.empty)
      } else {
        
        val placeholder = Fragment.const(List.fill(summon[Write[T]].length)("?").mkString(","))
        val sql = fr"INSERT INTO ${Fragment.const(tableName)} VALUES (DEFAULT,$placeholder)"
        
        Update[T](sql.query.sql)
          .updateMany(chunk.toList)
          .transact(xa)
          .map(_ => chunk)
      }
    }
  }
}