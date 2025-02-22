package com.yadavan88.dataplumber.effectful

import cats.effect.IO

import scala.util.control.NonFatal
trait DataPlumber[S, D] {
    def source: DataSource[S]
    def sink: DataSink[D]
    final def run = {
        (for {
            rows <- source.read
            transformed = transform(rows)
            _ <- sink.write(transformed)
        } yield ()).recoverWith {
            case NonFatal(error) => 
                IO.println(s"Error occurred while running DataPlumber: $error. Performing error handling hook") >> 
                handleError(error)
        }
    }

    /**
     * This function transform the source datastructure into the sink datastructure.
     */
    def transform(rows: List[S]): List[D]

    /**
     * This function handle the error depending on the requirement. 
     * For example, we can delete the inserted rows or log the error.
     */
    def handleError(error: Throwable): IO[Unit]

}

trait DataSource[S] {
    def read: IO[List[S]]
}

trait DataSink[D] {
    def write(rows: List[D]): IO[Unit]
}