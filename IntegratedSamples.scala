import scala.deriving.Mirror
import scala.compiletime.{erasedValue, summonInline}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import java.io.File
import scala.io.Source
import org.bson.Document
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import com.mongodb.client.MongoClients
import doobie.generic.auto.*
// trait CsvIntegrator[T] {
//   def location: String

//   inline def read(using m: Mirror.ProductOf[T]): List[T] = {
//     val file = new File(location)
//     val rows = Source
//       .fromFile(file)
//       .getLines()
//       .map(_.split(",").map(_.trim).toList)
//       .toList

//     rows.tail.map(row => fromCsvRow[T](row))
//   }
// }

case class Domain(name: String, count: Long, percentage: Double) 

class DomainCsvIntegrator(val location: String) extends CsvSource[Domain]

class DomainSimpleCsvIntegrator(val location: String) extends CsvSourceSimple[Domain] {
  override def fromCSVRow(row: String): Domain = {
    val fields = row.split(",").map(_.trim).toList
    Domain(fields(0), fields(1).toLong, fields(2).toDouble)
  }
}

class DomainMongoIntegrator(val collectionName: String)
    extends MongoSource[Domain] {

  override def mongoUri: String = "mongodb://localhost:27017/deviceident"

  override def fromDocument(doc: Document): Domain = {
    Domain(
      doc.getString("name"),
      doc.getLong("count"),
      doc.getDouble("percentage")
    )
  }
}

class DomainMongoWriter(val collectionName: String)
    extends MongoSink[Domain] {

  override def mongoUri: String = "mongodb://localhost:27017/deviceident"

  override def toDocument(value: Domain): Document = {
    val doc = new Document()
    doc.put("name", value.name)
    doc.put("count", value.count)
    doc.put("percentage", value.percentage)
    doc
  }
}

class DomainPostgresIntegrator(val tableName: String)
    extends PostgresSource[Domain] {

  override def connectionString: String =
    "jdbc:postgresql://localhost:5432/University?user=user&password=Password!2024"
}

class DomainCSVWriter(val location: String) extends CSVSink[Domain]

class DomainSimpleCSVWriter(val location: String) extends CSVSinkSimple[Domain] {

  override protected def headers: String = "name,count,percentage"

  override def toCSVRow(value: Domain): String = {
    s"${value.name},${value.count},${value.percentage}"
  }
}

@main
def main = {
  val csvSource = DomainCsvIntegrator(
    "/Users/yadukrishnankrishnan/source/integrator/domain.csv"
  )
  val domains: List[Domain] = csvSource.read
  println("reading from csv.... ")
  domains.foreach(println)

  println("reading from simple csv.... ")
  val simpleCsvSource = DomainSimpleCsvIntegrator(
    "/Users/yadukrishnankrishnan/source/integrator/domain.csv"
  )
  val domainsSimple: List[Domain] = simpleCsvSource.read
  domainsSimple.foreach(println)

  println("reading from mongodb.... ")
  val mongoIntegrator = DomainMongoIntegrator("domains")
  val domainsMongo: List[Domain] = mongoIntegrator.read
  domainsMongo.foreach(println)

  println("reading from postgresql.... ")

  val pgIntegrator = DomainPostgresIntegrator("domains")
  val domainsPG: List[Domain] = pgIntegrator.read
  domainsPG.foreach(println)


  println("writing to csv.... ")
  val csvWriter = DomainCSVWriter("/Users/yadukrishnankrishnan/source/integrator/domain-2.csv")
  val updatedDomains = domains.map(d => d.copy(count = d.count * 10, name = d.name + " updated"))
  csvWriter.write(updatedDomains)

  println("writing to simple csv.... ")
  val simpleCsvWriter = DomainSimpleCSVWriter("/Users/yadukrishnankrishnan/source/integrator/domain-3.csv")
  simpleCsvWriter.write(updatedDomains)

  println("writing to mongodb.... ")
  val mongoWriter = DomainMongoWriter("domains-new")
  mongoWriter.write(updatedDomains)

}
