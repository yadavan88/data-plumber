import com.mongodb.client.MongoClients
import scala.jdk.CollectionConverters._
import org.bson.Document
import com.mongodb.MongoClientSettings
import com.mongodb.ConnectionString
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

trait MongoSource[T] {
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

  def read: List[T] = {
    val res = collection.find().iterator().asScala.toList
    res.map(doc => fromDocument(doc))
  }
  protected def fromDocument(doc: Document): T
}
