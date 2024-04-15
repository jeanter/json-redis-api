package com.json.redis.exception;

/**
 * Created by GodzillaHua on 12/22/16.
 */
public class RedisClientException extends RuntimeException {
    public RedisClientException(String message) {
        super(message);
    }

    public RedisClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
