package com.yadavan88.dataplumber.streaming

import cats.effect.IO
import fs2.*
import scala.util.control.NonFatal
import _root_.com.yadavan88.dataplumber.offsetable.RedisClient
import _root_.com.yadavan88.dataplumber.offsetable.Offset
import _root_.com.yadavan88.dataplumber.offsetable.Offsetable
import cats.implicits._

trait StreamingDataPlumber[S, D] {
    def source: StreamingDataSource[S]
    def sink: StreamingDataSink[D]
    def name: String
    def redisHost: String
    def batchSize: Int

    private lazy val redisClient = new RedisClient(redisHost, name)
    private val OFFSET_KEY = "offset"

    final def run: IO[Unit] = {
        Stream.eval(redisClient.get(OFFSET_KEY))
          .flatMap { lastOffset =>
            val offset = lastOffset.map(off => Offset(off, java.time.LocalDateTime.now.toString))
            source.read(offset)
          }
          .chunkN(batchSize)
          .through(transform)
          .through(sink.write)
          .evalMap { lastOffset => 
              lastOffset.traverse(offset => redisClient.set(OFFSET_KEY, offset))
          }
          .handleErrorWith { error =>
              Stream.eval(
                  IO.println(s"Error occurred while running DataPlumber: $error. Performing error handling hook") >>
                  handleError(error)
              ) >> Stream.empty
          }
          .compile
          .drain
    }

    /**
     * This function transform the source datastructure into the sink datastructure.
     */
    def transform: Pipe[IO, Chunk[S], Chunk[D]]

    /**
     * This function handle the error depending on the requirement. 
     * For example, we can delete the inserted rows or log the error.
     */
    def handleError(error: Throwable): IO[Unit]
}

trait StreamingDataSource[S] {
    def read(offset: Option[Offset]): Stream[IO, S]
}

trait StreamingDataSink[D] {
    def write: Pipe[IO, Chunk[D], Option[String]]
}