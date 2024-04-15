package com.json.redis;

import com.json.redis.connection.Connection;

/**
 * Created by   on 12/23/16.
 */
public interface RedisLockProcessor<T> {

    T doInLock(Connection connection);

}
