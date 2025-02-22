package com.yadavan88.dataplumber.effectful.sink

import cats.effect.IO
import com.mongodb.client.MongoClients
import com.mongodb.{ConnectionString, MongoClientSettings}
import com.yadavan88.dataplumber.effectful.DataSink
import org.bson.Document

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

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

  def write(rows: List[T]): IO[Unit] = {
    val documents = rows.map(toDocument)
    IO(collection.insertMany(documents.asJava))
  }
  protected def toDocument(value: T): Document
}
