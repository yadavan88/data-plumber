package examples.streaming

import com.yadavan88.dataplumber.streaming.source.StreamingMockSource
import fs2.*
import cats.effect.IO
import scala.concurrent.duration.*
import java.time.LocalDateTime
import scala.util.Random
import java.time.LocalDate
import com.yadavan88.dataplumber.streaming.sink.StreamingPostgresSink
import doobie.util.Write
import com.yadavan88.dataplumber.streaming.StreamingDataPlumber
import doobie.util.meta.Meta
import examples.simple.StarLogEntry
import examples.simple.LogType
import cats.effect.IOApp
import java.util.UUID
import doobie.util.Read
import com.yadavan88.dataplumber.streaming.source.StreamingPostgresSource

class StarLogMockSource extends StreamingMockSource[StarLogEntry] {

  override def mockData: Stream[IO, StarLogEntry] = {
    Stream.fixedRate[IO](1.second).evalMap { _ =>
      IO(StarLogEntry(
        starDate = Random.between(47500, 49000),
        logType = LogType.PersonalLog,
        crewId = Random.between(1, 1000),
        entry = "Personal log entry, " + UUID.randomUUID().toString,
        planetaryDate = LocalDate.now(),
        starfleetTime = LocalDateTime.now()
      ))
    }
  }
}

class StarLogPostgresSink(using Write[PGStarLogEntry]) extends StreamingPostgresSink[PGStarLogEntry] {
  override def tableName: String = "starlog_streaming"
  override def connectionString: String = "jdbc:postgresql://localhost:5432/data-plumber?user=postgres&password=admin"
  override def columnNames: List[String] = List(
    "star_date",
    "log_type",
    "crew_id",
    "entry",
    "planetary_date",
    "starfleet_time"
  )
}

// class StarLogPostgresSource(using Read[StarLogEntry]) extends StreamingPostgresSource[StarLogEntry] {
//   override def tableName: String = "starlog_streaming"
//   override def connectionString: String = "jdbc:postgresql://localhost:5432/data-plumber?user=postgres&password=admin"
// }

case class PGStarLogEntry(
    id: Long,
    starDate: Double,
    logType: LogType,
    crewId: Int,
    entry: String,
    planetaryDate: LocalDate,
    starfleetTime: LocalDateTime
)

object StarLogStreamingPostgresWriter  {
  import doobie.implicits.javatimedrivernative.*
  given Meta[LogType] = Meta[String].imap(LogType.valueOf)(_.toString)
  given Write[PGStarLogEntry] = Write[(Double, String, Int, String, LocalDate, LocalDateTime)]
    .contramap(e => (
      e.starDate,    
      e.logType.toString, 
      e.crewId,      
      e.entry,      
      e.planetaryDate, 
      e.starfleetTime 
    ))
}

class MockToPostgresStarLogDataPlumber extends StreamingDataPlumber[StarLogEntry, PGStarLogEntry] {
  import StarLogStreamingPostgresWriter.given
  override val source: StreamingMockSource[StarLogEntry] = new StarLogMockSource()
  override val sink: StreamingPostgresSink[PGStarLogEntry] = new StarLogPostgresSink()
  override val name: String = "starlog-mock-to-postgres"
  override val redisHost: String = "redis://localhost:6379"
  override val batchSize: Int = 5
  override def handleError(error: Throwable): IO[Unit] = IO.unit
  override def transform: Pipe[IO, Chunk[StarLogEntry], Chunk[PGStarLogEntry]] = _.map(chunk => chunk.map( v =>PGStarLogEntry(0L, v.starDate, v.logType, v.crewId, v.entry, v.planetaryDate, v.starfleetTime)))
}

object StreamingDataPlumberApp extends IOApp.Simple {
  val mockToPg = new MockToPostgresStarLogDataPlumber()
  def run: IO[Unit] = 
    IO.println("Starting streaming data plumber...") >>
    mockToPg.run
}