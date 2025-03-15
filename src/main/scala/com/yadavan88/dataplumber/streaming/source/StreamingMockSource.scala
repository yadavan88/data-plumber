package com.yadavan88.dataplumber.streaming.source

import fs2.*
import cats.effect.IO
import _root_.com.yadavan88.dataplumber.streaming.StreamingDataSource
import _root_.com.yadavan88.dataplumber.offsetable.Offset

trait StreamingMockSource[T] extends StreamingDataSource[T] {
  def mockData: Stream[IO, T]
  def read(offset: Option[Offset]): Stream[IO, T] = {
    mockData
  }
}