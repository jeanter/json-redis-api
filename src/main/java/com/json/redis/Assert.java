package com.json.redis;

import com.json.redis.exception.RedisClientException;

/**
 * Created by   on 12/23/16.
 */
public abstract class Assert {

    public static void notNull(Object obj) {
        if (obj == null) {
            throw new RedisClientException("obj cannot be null");
        }
    }
}
