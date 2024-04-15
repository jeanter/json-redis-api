package com.json.redis.connection.factory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RedissonClient;

import com.json.redis.conf.RedisConfiguration;
import com.json.redis.conf.RedisShardedConfiguration;
import com.json.redis.connection.Connection;
import com.json.redis.connection.ConnectionUtils;
import com.json.redis.connection.RedisServer;
import com.json.redis.connection.ShardedConnection;
import com.json.redis.exception.RedisClientException;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by   on 12/22/16.
 */
public class ShardedConnectionFactory extends AbstractConnectionFactory {

    private ShardedJedisPool shardedJedisPool;

    public ShardedConnectionFactory(RedisConfiguration configuration){
        super(configuration);
        List<RedisServer> servers = ConnectionUtils.getRedisServerList(getConfiguration().getIpAddressList());
        if (servers == null || servers.size() == 0) {
            throw new RedisClientException("cluster server cannot be null");
        }
        List<JedisShardInfo> shards = new ArrayList<>();
        for(RedisServer server : servers) {
            JedisShardInfo jedisShardInfo = new JedisShardInfo(server.getHost(), server.getPort(), getConfiguration().getTimeout());
            if (StringUtils.isNotEmpty(configuration.getPassword())) {
                jedisShardInfo.setPassword(configuration.getPassword());
            }
            shards.add(jedisShardInfo);
        }
        shardedJedisPool = new ShardedJedisPool(config, shards);
    }

    @Override
    public Connection makeConnection(RedissonClient redissonClient) {
        return new ShardedConnection(shardedJedisPool.getResource(), redissonClient);
    }

    @Override
    public void close() {
        super.close();
        if (shardedJedisPool != null && !shardedJedisPool.isClosed()){
            IOUtils.closeQuietly(shardedJedisPool);
        }
    }
}
