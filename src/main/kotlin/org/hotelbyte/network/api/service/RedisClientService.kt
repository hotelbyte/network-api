package org.hotelbyte.network.api.service

import io.vertx.core.Vertx
import io.vertx.kotlin.redis.RedisOptions
import io.vertx.redis.RedisClient

/**
 * Wrapper to keep centralized the redis connection
 */
open class RedisClientService(_vertx: Vertx) {

    private val config = RedisOptions(host = "localhost")

    val vertx = _vertx
    var redis:RedisClient? = null

    /**
     * Connection establishment
     * TODO remove the return?
     * return redis
     */
    fun connect():RedisClient? {
        redis = RedisClient.create(vertx, config)
        return redis
    }
}