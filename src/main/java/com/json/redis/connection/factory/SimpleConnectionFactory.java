package com.json.redis.connection.factory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RedissonClient;

import com.json.redis.conf.RedisConfiguration;
import com.json.redis.conf.RedisSingleConfiguration;
import com.json.redis.connection.Connection;
import com.json.redis.connection.ConnectionUtils;
import com.json.redis.connection.RedisServer;
import com.json.redis.connection.SimpleConnection;
import com.json.redis.exception.RedisClientException;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

import java.util.List;

/**
 * Created by   on 12/22/16.
 */
public class SimpleConnectionFactory extends AbstractConnectionFactory {

    private JedisPool jedisPool;

    public SimpleConnectionFactory(RedisConfiguration configuration) {
        super(configuration);
        List<RedisServer> list = ConnectionUtils.getRedisServerList(configuration.getIpAddressList());
        if (list == null || list.size() == 0) {
            throw new RedisClientException("please set redis host name port");
        }

        if (StringUtils.isEmpty(configuration.getPassword())) {
            this.jedisPool = new JedisPool(config, list.get(0).getHost(), list.get(0).getPort(), configuration.getTimeout());
        } else {
            this.jedisPool = new JedisPool(config, list.get(0).getHost(), list.get(0).getPort(), configuration.getTimeout(), configuration.getPassword(), Protocol.DEFAULT_DATABASE);
        }

    }

    @Override
    public Connection makeConnection(RedissonClient redissonClient) {
        return new SimpleConnection(jedisPool.getResource(), redissonClient);
    }

    @Override
    public void close() {
        super.close();
        if (jedisPool != null && !jedisPool.isClosed()) {
            IOUtils.closeQuietly(jedisPool);
        }
    }
}
