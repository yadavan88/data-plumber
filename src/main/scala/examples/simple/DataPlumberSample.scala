package examples.simple

import com.yadavan88.dataplumber.simple.DataPlumber
import com.yadavan88.dataplumber.simple.source.*
import com.yadavan88.dataplumber.simple.sink.*
import org.bson.Document

import java.time.{LocalDate, LocalDateTime}
enum LogType {
  case CaptainsLog, FirstOfficerLog, ChiefMedicalLog, ChiefEngineerLog,
    PersonalLog
}
case class StarLogEntry(
    starDate: Double,
    logType: LogType,
    crewId: Int,
    entry: String,
    planetaryDate: LocalDate,
    starfleetTime: LocalDateTime
)

case class MongoStarLogEntry(
    starDate: Double,
    logType: String,
    crewId: Int,
    entry: String,
    planetaryDate: LocalDate,
    starfleetTime: LocalDateTime
)

class StarLogSource extends CsvSource[StarLogEntry] {

  override protected def fromCSVRow(row: String): StarLogEntry =
    row.split(",").map(_.trim) match {
      case Array(
            starDate,
            logType,
            crewId,
            entry,
            planetaryDate,
            starfleetTime
          ) =>
        StarLogEntry(
          starDate.toDouble,
          LogType.valueOf(logType),
          crewId.toInt,
          entry,
          LocalDate.parse(planetaryDate),
          LocalDateTime.parse(starfleetTime)
        )
      case _ =>
        throw new IllegalArgumentException(s"Invalid CSV row format: $row")
    }

  override def location: String =
    "/Users/yadukrishnankrishnan/source/integrator/starlog.csv"
}

class StarLogSink extends MongoSink[MongoStarLogEntry] {
  override def collectionName: String = "starlog"
  override def mongoUri: String = "mongodb://localhost:27017/starlog"
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

class StarLogIntegrator extends DataPlumber[StarLogEntry, MongoStarLogEntry] {
  override def source: CsvSource[StarLogEntry] = new StarLogSource()
  override def sink: MongoSink[MongoStarLogEntry] = new StarLogSink()
  override def transform(list: List[StarLogEntry]): List[MongoStarLogEntry] = {
    list.map(value =>
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
}

@main
def start = {
  val integrator = new StarLogIntegrator()
  integrator.run
}
