package com.yadavan88.dataplumber.effectful.source

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*

trait PostgresSource[T] {
  def tableName: String
  def connectionString: String

  def read(implicit read: Read[T]): IO[List[T]] = {
    val xa = Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      connectionString,
      "username",
      "password",
      None
    )

    val query = fr"SELECT * FROM" ++ Fragment.const(tableName)
    query
      .query[T] 
      .to[List]
      .transact(xa)
  }
}