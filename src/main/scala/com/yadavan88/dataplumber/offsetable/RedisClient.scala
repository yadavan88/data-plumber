package com.yadavan88.dataplumber.offsetable

import cats.effect.*
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.effect.Log.Stdout.given

class RedisClient(host: String, prefix: String) {
  private val redisClient = Redis[IO].utf8(host)

  private def buildKey(key: String) = s"$prefix-$key"

  def get(key: String) = redisClient.use { redis =>
    redis.get(buildKey(key))
  }

  def set(key: String, value: String) = redisClient.use { redis =>
    redis.set(buildKey(key), value)
  }

}
