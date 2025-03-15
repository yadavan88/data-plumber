package com.yadavan88.dataplumber.streaming.sink

import cats.effect.IO
import com.mongodb.client.MongoClients
import com.mongodb.{ConnectionString, MongoClientSettings}
import com.yadavan88.dataplumber.offsetable.DataSink
import org.bson.Document

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import com.yadavan88.dataplumber.offsetable.Offset
import java.time.LocalDateTime
import com.yadavan88.dataplumber.streaming.StreamingDataSink
import fs2.Pipe
import fs2.Chunk

trait StreamingMongoSink[T] extends StreamingDataSink[T] {
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

  def write: Pipe[IO, Chunk[T], Option[String]] = { stream =>
    stream.evalMap { chunk =>
      if (chunk.isEmpty) {
        IO.pure(None)
      } else {
        val documents = chunk.toList.map(toDocument)
        IO(collection.insertMany(documents.asJava))
          .map(_ => chunk.toList.lastOption.map(_.toString))
      }
    }
  }
  protected def toDocument(value: T): Document
}
