import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import cats.effect.IO
import cats.effect.unsafe.implicits.global

trait PostgresSource[T] {
  def tableName: String
  def connectionString: String

  def read(implicit read: Read[T]): List[T] = {
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
      .unsafeRunSync()
  }
}