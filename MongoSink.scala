import com.mongodb.client.MongoClients
import scala.jdk.CollectionConverters._
import org.bson.Document
import com.mongodb.MongoClientSettings
import com.mongodb.ConnectionString
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

trait MongoSink[T] extends DataSink[T] {
  def collectionName: String
  def mongoUri: String

  private val connectionString = new ConnectionString(mongoUri)
  private val settings = MongoClientSettings
    .builder()
    .applyConnectionString(connectionString)
    .applyToSocketSettings(builder =>
      builder.connectTimeout(5.seconds.toMillis.toInt, TimeUnit.MILLISECONDS)
    )
    .build()
    
  private val mongoClient = MongoClients.create(settings)
  private val database = mongoClient.getDatabase(connectionString.getDatabase)
  private val collection = database.getCollection(collectionName)

  def write(rows: List[T]): Unit = {
    val documents = rows.map(toDocument)
    collection.insertMany(documents.asJava)
  }
  protected def toDocument(value: T): Document
}
