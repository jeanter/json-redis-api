package com.json.redis.connection.factory;

import com.json.redis.connection.Connection;

/**
 *
 * Created by   on 12/21/16.
 */
public interface ConnectionFactory {

    Connection createConnection();

    void close();

}
