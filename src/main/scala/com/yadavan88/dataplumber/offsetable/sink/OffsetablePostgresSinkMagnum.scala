package com.yadavan88.dataplumber.offsetable.sink

import com.yadavan88.dataplumber.offsetable.DataSink
import cats.effect.IO
import java.time.LocalDateTime
import com.yadavan88.dataplumber.offsetable.Offset
import com.augustnagro.magnum.Repo
import com.augustnagro.magnum.DbCon
import com.augustnagro.magnum.*
import com.yadavan88.dataplumber.offsetable.Offsetable

trait OffsetablePostgresSinkMagnum[T] extends DataSink[T] {

  implicit val repo: Repo[T, T, Long]
  def tableName: String
  def connectionString: String

  private val dataSource: javax.sql.DataSource = {
    val ds = new org.postgresql.ds.PGSimpleDataSource()
    ds.setURL(connectionString)
    ds
  }
  private val xa = Transactor(dataSource)

  def write(rows: List[T], readOffset: Option[Offset]): IO[Unit] = {
    IO {
      connect(xa) {
        repo.insertAll(rows)
      }
    }
  }

}
