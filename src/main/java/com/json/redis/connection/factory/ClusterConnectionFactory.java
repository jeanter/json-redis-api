package com.json.redis.connection.factory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RedissonClient;

import com.json.redis.BatchModel;
import com.json.redis.conf.RedisClusterConfiguration;
import com.json.redis.conf.RedisConfiguration;
import com.json.redis.connection.ClusterConnection;
import com.json.redis.connection.Connection;
import com.json.redis.connection.ConnectionUtils;
import com.json.redis.connection.RedisServer;
import com.json.redis.exception.RedisClientException;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by   on 12/22/16.
 */
public class ClusterConnectionFactory extends AbstractConnectionFactory {

    private JedisCluster cluster;

    public ClusterConnectionFactory(RedisConfiguration configuration) {
        super(configuration);
        List<RedisServer> servers = ConnectionUtils.getRedisServerList(getConfiguration().getIpAddressList());
        if (servers == null) {
            throw new RedisClientException("cluster server cannot be null");
        }
        Set<HostAndPort> hostAndPortList = new HashSet<>(servers.size());
        for (RedisServer server : servers) {
            hostAndPortList.add(new HostAndPort(server.getHost(), server.getPort()));
        }

        if (StringUtils.isEmpty(configuration.getPassword())) {
            cluster = new JedisCluster(hostAndPortList, getConfiguration().getTimeout(), config);
        } else {
            cluster = new JedisCluster(hostAndPortList, configuration.getTimeout(), configuration.getSoTimeout(), configuration.getMaxAttempts(), configuration.getPassword(), config);
        }


    }

    public Connection makeConnection(RedissonClient redissonClient) {
        int batchModel = getConfiguration().getBatchModel();
        return new ClusterConnection(cluster, redissonClient, BatchModel.getModelByValue(batchModel));
    }

    @Override
    public void close() {
        super.close();
        if (cluster != null) {
            IOUtils.closeQuietly(cluster);
        }
    }
}
