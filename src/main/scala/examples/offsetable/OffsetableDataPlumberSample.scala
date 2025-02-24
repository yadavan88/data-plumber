package examples.offsetable

import examples.effectful.StarLogEntry
import examples.effectful.MongoStarLogEntry
import com.yadavan88.dataplumber.offsetable.OffsetableDataPlumber
import com.yadavan88.dataplumber.offsetable.DataSource
import com.yadavan88.dataplumber.offsetable.DataSink
import com.yadavan88.dataplumber.offsetable.source.OffsetableCsvSource
import com.yadavan88.dataplumber.*
import com.yadavan88.dataplumber.offsetable.sink.OffsetableMongoSink
import examples.effectful.LogType
import org.bson.Document
import cats.effect.IO
import java.time.LocalDateTime
import com.yadavan88.dataplumber.offsetable.ReadResult
import com.yadavan88.dataplumber.offsetable.source.OffsetableMongoSource
import java.time.LocalDate
import com.yadavan88.dataplumber.offsetable.sink.OffsetablePostgresSink
import doobie.util.Write
import doobie.util.meta.Meta
import doobie.util.Put
import java.time.ZoneOffset
import com.yadavan88.dataplumber.offsetable.sink.OffsetablePostgresSinkMagnum
import com.augustnagro.magnum.Repo


class StarLogOffsetableCsvSource extends OffsetableCsvSource[StarLogEntry] {

  override protected def fromCSVRow(row: String): StarLogEntry =
    GenericCSVParser.fromCsvRow[StarLogEntry](row.split(",").toList)

  implicit val logTypeParser: CsvParser[LogType] = (str: String) =>
    LogType.valueOf(str)

  override def location: String =
    "/Users/yadukrishnankrishnan/personal/source/data-plumber/data/starlog.csv"

}

class StarLogOffsetableMongoSink extends OffsetableMongoSink[MongoStarLogEntry] {

  override def mongoUri: String = "mongodb://mongoadmin:mongopassword@localhost:27027/starlog?authMechanism=SCRAM-SHA-256&authSource=admin"  
  override def collectionName: String = "starlog-offsetable"

   override def toDocument(value: MongoStarLogEntry): org.bson.Document = {
    val doc = new Document()
    doc.put("starDate", value.starDate)
    doc.put("logType", value.logType)
    doc.put("crewId", value.crewId)
    doc.put("entry", value.entry)
    doc.put("planetaryDate", value.planetaryDate)
    doc.put("starfleetTime", value.starfleetTime)
    doc
  }
}

class StarLogOffsetableMongoSource extends OffsetableMongoSource[MongoStarLogEntry] {

  // reducing the batch size to 2 for demo
  override def batchSize: Int = 2

  override protected def fromDocument(doc: Document): MongoStarLogEntry = MongoStarLogEntry(
    doc.getDouble("starDate"),
    doc.getString("logType"),
    doc.getInteger("crewId"),
    doc.getString("entry"),
    doc.getDate("planetaryDate").toInstant.atZone(ZoneOffset.UTC).toLocalDate,
    doc.getDate("starfleetTime").toInstant.atZone(ZoneOffset.UTC).toLocalDateTime
  )

  override def mongoUri: String = "mongodb://mongoadmin:mongopassword@localhost:27027/starlog?authMechanism=SCRAM-SHA-256&authSource=admin"  
  override def collectionName: String = "starlog-offsetable"
}


// class StarLogOffsetablePostgresSink(using Write[StarLogEntry]) extends PostgresSink[StarLogEntry] {
//   override def connectionString: String = "jdbc:postgresql://localhost:5432/data-plumber?user=postgres&password=admin"
//   override def tableName: String = "starlog_offsetable"
// }

class StarLogOffsetablePostgresSink(using Write[StarLogEntry]) extends OffsetablePostgresSinkMagnum[StarLogEntry] {

  override implicit val repo: Repo[StarLogEntry, StarLogEntry, Long] = Repo[StarLogEntry, StarLogEntry, Long]

  override def connectionString: String = "jdbc:postgresql://localhost:5432/data-plumber?user=postgres&password=admin"
  override def tableName: String = "starlogentry"  // I don't know how to set the name to "starlog_offsetable" in repo
}

// *********************** Sample DataPlumber Implementation *********************************** 

class OffsetableCsvToMongoPlumber
    extends OffsetableDataPlumber[StarLogEntry, MongoStarLogEntry] {

  override def transform(readResult: ReadResult[StarLogEntry]): List[MongoStarLogEntry] = {
    readResult.rows.map(value =>
      MongoStarLogEntry(
        value.starDate,
        value.logType.toString,
        value.crewId,
        value.entry,
        value.planetaryDate,
        value.starfleetTime
      )
    )
  }

  override def handleError(error: Throwable): IO[Unit] = {
    error.printStackTrace()
    IO.println(s"Error from offsetable plumber: $error")
  }

  override def name: String = "offsetable-plumber-sample"
  override def redisHost: String = "redis://localhost:6379"
  override def source: DataSource[StarLogEntry] = new StarLogOffsetableCsvSource
  override def sink: DataSink[MongoStarLogEntry] = new StarLogOffsetableMongoSink
}

object StarLogOffsetablePostgresSinkWriter  {
  import doobie.implicits.javatimedrivernative.*
  given Meta[LogType] = Meta[String].imap(LogType.valueOf)(_.toString)
  given Write[StarLogEntry] = Write[(Double, String, Int, String, LocalDate, LocalDateTime)]
    .contramap(e => (e.starDate, e.logType.toString, e.crewId, e.entry, e.planetaryDate, e.starfleetTime))
}

class OffsetableMongoToPostgresDataPlumber extends OffsetableDataPlumber[MongoStarLogEntry, StarLogEntry] {

  import StarLogOffsetablePostgresSinkWriter.given

  override def source: DataSource[MongoStarLogEntry] = new StarLogOffsetableMongoSource

  override def sink: DataSink[StarLogEntry] = new StarLogOffsetablePostgresSink
    //new StarLogOffsetablePostgresSink

  override def name: String = "mongo-to-postgres-plumber"

  override def redisHost: String = "redis://localhost:6379"

  override def transform(readResult: ReadResult[MongoStarLogEntry]): List[StarLogEntry] = {
    readResult.rows.map(value =>
      StarLogEntry(
        value.starDate,
        LogType.valueOf(value.logType),
        value.crewId,
        value.entry,
        value.planetaryDate,
        value.starfleetTime
      )
    )
  }

  override def handleError(error: Throwable): IO[Unit] = {
    error.printStackTrace()
    IO.println(s"Error from offsetable plumber: $error")
  }
}

import cats.effect.unsafe.implicits.global

@main
def startOffsetablePlumber = {
  import cats.syntax.*
  val csvToMongo = new OffsetableCsvToMongoPlumber()
  csvToMongo.run.unsafeRunSync() 
  val mongoToPG = new OffsetableMongoToPostgresDataPlumber()

  def processUntilEmpty: IO[Unit] = for {
    count <- mongoToPG.run
    _ <- IO.println(s"Processed $count records")
    _ <- if (count > 0) processUntilEmpty else IO.println("Processing complete!")
  } yield ()

  processUntilEmpty.unsafeRunSync()

}
