package com.yadavan88.dataplumber.effectful.source

import cats.effect.IO
import com.mongodb.client.MongoClients
import com.mongodb.{ConnectionString, MongoClientSettings}
import org.bson.Document

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

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

  def read: IO[List[T]] = {
    val res = collection.find().iterator().asScala.toList
    IO(res.map(doc => fromDocument(doc)))
  }
  protected def fromDocument(doc: Document): T
}
