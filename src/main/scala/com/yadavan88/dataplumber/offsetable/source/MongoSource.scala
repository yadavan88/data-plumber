package com.yadavan88.dataplumber.offsetable.source

import cats.effect.IO
import com.mongodb.client.MongoClients
import com.mongodb.{ConnectionString, MongoClientSettings}
import org.bson.Document

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import com.yadavan88.dataplumber.offsetable.Offset
import com.yadavan88.dataplumber.offsetable.DataSource
import com.mongodb.client.model.Sorts
import com.yadavan88.dataplumber.offsetable.ReadResult
import java.time.LocalDateTime

trait MongoSource[T] extends DataSource[T] {
  def collectionName: String
  def mongoUri: String
  def batchSize: Int = 100

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
  protected def fromDocument(doc: Document): T
}
