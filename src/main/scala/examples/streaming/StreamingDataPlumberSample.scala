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
import com.yadavan88.dataplumber.offsetable.Offsetable
import com.yadavan88.dataplumber.streaming.sink.StreamingMongoSink
import org.bson.Document
import cats.syntax.parallel.catsSyntaxParallelSequence1

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

class StarLogPostgresSource(using Read[PGStarLogEntry]) extends StreamingPostgresSource[PGStarLogEntry] {
  override def tableName: String = "starlog_streaming"
  override def connectionString: String = "jdbc:postgresql://localhost:5432/data-plumber?user=postgres&password=admin"
}

class StarLogMongoSink extends StreamingMongoSink[PGStarLogEntry] {

  override protected def toDocument(value: PGStarLogEntry): Document = {
    Document.parse(s"""{
      "star_date": ${value.starDate},
      "log_type": "${value.logType}",
      "crew_id": ${value.crewId},
      "entry": "${value.entry}",
      "planetary_date": "${value.planetaryDate}",
      "starfleet_time": "${value.starfleetTime}"
    }""")
  }

  override lazy val collectionName: String = "starlog_streaming"
  override lazy val mongoUri: String = "mongodb://mongoadmin:mongopassword@localhost:27027/starlog?authMechanism=SCRAM-SHA-256&authSource=admin"  

}

case class PGStarLogEntry(
    id: Long,
    starDate: Double,
    logType: LogType,
    crewId: Int,
    entry: String,
    planetaryDate: LocalDate,
    starfleetTime: LocalDateTime
) extends Offsetable

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

  given Read[PGStarLogEntry] = Read[(Long, Double, String, Int, String, LocalDate, LocalDateTime)].map{ v =>
    PGStarLogEntry(v._1, v._2, LogType.valueOf(v._3), v._4, v._5, v._6, v._7)
  }
}

class MockToPostgresStarLogDataPlumber extends StreamingDataPlumber[StarLogEntry, PGStarLogEntry] {
  import StarLogStreamingPostgresWriter.given
  override val source: StreamingMockSource[StarLogEntry] = new StarLogMockSource()
  override val sink: StreamingPostgresSink[PGStarLogEntry] = new StarLogPostgresSink()
  override val name: String = "mock-to-pg-streaming-starlog"
  override val redisHost: String = "redis://localhost:6379"
  override val batchSize: Int = 5
  override def handleError(error: Throwable): IO[Unit] = IO.unit
  override def transform: Pipe[IO, Chunk[StarLogEntry], Chunk[PGStarLogEntry]] = _.map(chunk => chunk.map( v =>PGStarLogEntry(0L, v.starDate, v.logType, v.crewId, v.entry, v.planetaryDate, v.starfleetTime)))
}


class PostgresToMongoStarLogDataPlumber extends StreamingDataPlumber[PGStarLogEntry, PGStarLogEntry] {
  import StarLogStreamingPostgresWriter.given
  override val source: StreamingPostgresSource[PGStarLogEntry] = new StarLogPostgresSource()
  override val sink: StreamingMongoSink[PGStarLogEntry] = new StarLogMongoSink()
  override val name: String = "pg-to-mongo-streamingstarlog"
  override val redisHost: String = "redis://localhost:6379"
  override val batchSize: Int = 30
  override val batchTimeout: FiniteDuration = 10.seconds
  override def handleError(error: Throwable): IO[Unit] = IO.unit
  override def transform: Pipe[IO, Chunk[PGStarLogEntry], Chunk[PGStarLogEntry]] = _.map(chunk => chunk)
}

object StreamingDataPlumberApp extends IOApp.Simple {
  val mockToPg = new MockToPostgresStarLogDataPlumber()
  val pgToMongo = new PostgresToMongoStarLogDataPlumber()
  def run: IO[Unit] = 
    IO.println("Starting streaming data plumber..") >>
    List(
      mockToPg.run,
      pgToMongo.run
    ).parSequence.void
}