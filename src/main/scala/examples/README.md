# Data Plumber Framework - Quick Start Guide


# Simple Data Plumber 

Let's walk through creating a data pipeline using the Star Trek logs example from the sample code. This doesn't use any effect systems like IO. 

## Step 1: Create a Source

Extend `CsvSource` to read Star Trek logs from a CSV file:

```scala
class StarLogSource extends CsvSource[StarLogEntry] {
  override def location: String = "/path/to/starlog.csv"
  
  override protected def fromCSVRow(row: String): StarLogEntry = {
    row.split(",").map(_.trim) match {
      case Array(starDate, logType, crewId, entry, planetaryDate, starfleetTime) =>
        StarLogEntry(
          starDate.toDouble,
          LogType.valueOf(logType),
          crewId.toInt,
          entry,
          LocalDate.parse(planetaryDate),
          LocalDateTime.parse(starfleetTime)
        )
      case _ => throw new IllegalArgumentException(s"Invalid CSV row format: $row")
    }
  }
}
```
Here this uses a simple read line and convert to case class. You may also look at the [Generic CsvParser](../com/yadavan88/dataplumber/CsvParser.scala) to easily convert simple csv files without manual work.

## Step 2: Create a Sink

Extend `MongoSink` to write logs to MongoDB:

```scala
class StarLogSink extends MongoSink[MongoStarLogEntry] {
  override def collectionName: String = "starlog"
  override def mongoUri: String = "mongodb://localhost:27017/starlog"
  
  override def toDocument(value: MongoStarLogEntry): Document = {
    val doc = new Document()
    doc.put("starDate", value.starDate)
    // .... add all fields 
    doc
  }
}
```


## Step 3: Create the Pipeline

Combine source and sink by extending `DataPlumber`:

```scala
class StarLogIntegrator extends DataPlumber[StarLogEntry, MongoStarLogEntry] {
  override def source: CsvSource[StarLogEntry] = new StarLogSource()
  override def sink: MongoSink[MongoStarLogEntry] = new StarLogSink()
  
  override def transform(list: List[StarLogEntry]): List[MongoStarLogEntry] = {
    list.map(value => MongoStarLogEntry(
      value.starDate,
      value.logType.toString,  // Convert enum to string
      value.crewId,
      value.entry,
      value.planetaryDate,
      value.starfleetTime
    ))
  }
}
```


## Step 4: Run the Pipeline

Create an entry point to run your pipeline:

```scala
@main
def start(): Unit = {
  val integrator = new StarLogIntegrator()
  integrator.run
}
```

## Notes 
When you run this, it reads the provided csv file and write the data into mongodb collection. We can extend the sources and sinks based on the requirement. 
For example, S3CsvSink that reads and csv from the S3 bucket and writes to Mongo or Postgres database, or a REST Endpoint Sink that posts this data. 

# Effectful Data Plumber 
This uses cats effect IO for handling the operations, which we can potentially extend to support retries and error handling in more easier way.

This is very similar to the simple version, but uses cats effect. So, not adding the sample code here.

# Offsetable Data Plumber Framework

The Offsetable version of Data Plumber provides functionality to track and resume data processing from the last processed position. 
This is particularly useful for batch processing or handling large datasets.

## Step 1: Define Your Data Models

Use the same data models as before.

## Step 2: Create an Offsetable Source

Extend the appropriate source trait (e.g., `CsvSource` or `MongoSource`):

```scala
class StarLogOffsetableCsvSource extends CsvSource[StarLogEntry] {
  override protected def fromCSVRow(row: String): StarLogEntry =
    GenericCSVParser.fromCsvRow[StarLogEntry](row.split(",").toList)

  override def location: String = "/path/to/starlog.csv"
}
```

In the source, we read the data and also it generates the next offset. If the source supports batch read, we can set the offset, if not, we can send empty value as offset. 
The source then checks this offset in the next run to build the query. 
For example, we can look at a MongoSource that tracks offset for reference:
```scala
trait MongoSource[T] extends DataSource[T] {
...
...
...

  def read(offset: Option[Offset]): IO[ReadResult[T]] = {
    IO {
      val filter = offset
        .map { off =>
          Document.parse(
            s"""{ "_id": { "$$gt": ObjectId("${off.lastOffset}") } }"""
          )
        }
        .getOrElse(new Document())

      val res = collection
        .find(filter)
        .sort(Sorts.ascending("_id"))
        .limit(batchSize)
        .iterator()
        .asScala
        .toList

      val nextOffset = res.lastOption.map(doc => Offset(doc.get("_id").toString, LocalDateTime.now().toString))
      ReadResult(res.map(doc => fromDocument(doc)), nextOffset)
    }
  }
}
```
This uses the offset value and build the next query. 
And then we can build the Source Implemantation as:
```scala
class StarLogOffsetableMongoSource extends MongoSource[MongoStarLogEntry] {

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
```
This uses the offset and batchsize

## Step 3: Create an Offsetable Sink

Extend the appropriate sink trait (e.g., `MongoSink` or `PostgresSink`):

```scala
class StarLogOffsetableMongoSink extends MongoSink[MongoStarLogEntry] {
  override def mongoUri: String = "mongodb://mongoadmin:mongopassword@localhost:27027/starlog"
  override def collectionName: String = "starlog-offsetable"

  override def toDocument(value: MongoStarLogEntry): Document = {
    val doc = new Document()
    doc.put("starDate", value.starDate)
    ... // add all fields
    doc
  }
}
```

## Step 4: Create the Offsetable Pipeline

Extend `OffsetableDataPlumber` and implement the required methods:

```scala
class OffsetableCsvToMongoPlumber extends OffsetableDataPlumber[StarLogEntry, MongoStarLogEntry] {
  // Define the transformation between source and target models
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

  // Handle errors that might occur during processing
  override def handleError(error: Throwable): IO[Unit] = {
    error.printStackTrace()
    IO.println(s"Error from offsetable plumber: $error")
  }

  // Configure pipeline properties
  override def name: String = "offsetable-plumber-sample"
  override def redisHost: String = "redis://localhost:6379"
  override def source: DataSource[StarLogEntry] = new StarLogOffsetableCsvSource
  override def sink: DataSink[MongoStarLogEntry] = new StarLogOffsetableMongoSink
}
```

If the sink supports offset(batch) read, it provides the new offst value after the read. Once the sink is processed successfully, the DataPlumber sets the new offset to redis.
The source can then use this offset for the next read/iteration.

## Step 5: Run the Pipeline

Create an entry point to run your offsetable pipeline:

```scala
import cats.effect.unsafe.implicits.global

@main
def startOffsetablePlumber = {
  val plumber = new OffsetableCsvToMongoPlumber()
  plumber.run.unsafeRunSync()
}
```

## Key Differences from Simple Version

1. **Offset Tracking**: The framework automatically tracks processing progress using Redis
2. **Batch Processing**: Supports processing data in batches
3. **Error Handling**: Includes explicit error handling with effects
4. **Resumability**: Can resume processing from last successful position
5. **Effect System**: Uses `cats.effect.IO` for effect handling

## Notes

- The offsetable version requires Redis for storing offset information
- Configure `redisHost` to point to your Redis instance
- The `transform` method now receives a `ReadResult` instead of a simple List
- Error handling is more explicit with the `handleError` method
- The pipeline runs within an effect context (IO)
- You can configure batch sizes in sources (e.g., `override def batchSize: Int = 2`)

This provides a robust way to create resumable data pipelines with offset tracking and proper error handling.



