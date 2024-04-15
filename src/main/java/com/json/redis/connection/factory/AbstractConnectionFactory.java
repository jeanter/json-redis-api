package com.json.redis.connection.factory;

import com.alibaba.fastjson.JSON;
import com.json.redis.conf.RedisClusterConfiguration;
import com.json.redis.conf.RedisConfiguration;
import com.json.redis.conf.RedisShardedConfiguration;
import com.json.redis.conf.RedisSingleConfiguration;
import com.json.redis.connection.Connection;
import com.json.redis.connection.ConnectionUtils;
import com.json.redis.connection.RedisServer;
import com.json.redis.exception.RedisClientException;
import com.json.redis.serializer.Deserializer;
import com.json.redis.serializer.Serializer;
import com.json.redis.serializer.codec.RedissonCodec;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by   on 3/21/17.
 */
public abstract class AbstractConnectionFactory implements ConnectionFactory {

    private static Logger logger = LoggerFactory.getLogger(AbstractConnectionFactory.class);

    private final RedisConfiguration configuration;
    private boolean closed = false;
    protected RedissonClient redissonClient;
    protected GenericObjectPoolConfig config = new GenericObjectPoolConfig();

    public AbstractConnectionFactory(RedisConfiguration configuration) {
        logger.error("AbstractConnectionFactory redissonConfig model  "+ configuration.getModel() );
        this.configuration = configuration;
        List<RedisServer> servers = ConnectionUtils.getRedisServerList(configuration.getIpAddressList());
        if (servers  != null && servers.size() > 0){
            logger.info("create lock redis connection");
            Config redissonConfig = new Config();

            if (configuration.getModel() ==1) {
                SingleServerConfig singleServerConfig = redissonConfig.useSingleServer();
                singleServerConfig.setSubscriptionsPerConnection(configuration.getMaxTotal());
                singleServerConfig.setConnectionPoolSize(configuration.getMaxTotal());
                singleServerConfig.setConnectionMinimumIdleSize(configuration.getMinIdle());
                singleServerConfig.setConnectTimeout(configuration.getTimeout());
                singleServerConfig.setRetryInterval(configuration.getRetryInterval());
                singleServerConfig.setTimeout(configuration.getTimeout());
                String address = servers.get(0).toString();
            	if(!address.startsWith("redis://")) {
            		address = "redis://"+address;
            	}
                singleServerConfig.setAddress(address);
                logger.error("redissonConfig model  "+ configuration.getModel() + " address : "+address);
                if (StringUtils.isNotEmpty(configuration.getPassword())) {
                    singleServerConfig.setPassword(configuration.getPassword());
                }

            } else if (configuration.getModel() ==0) {
                ClusterServersConfig clusterServersConfig = redissonConfig.useClusterServers();
                clusterServersConfig.setSubscriptionsPerConnection(configuration.getMaxTotal());
                clusterServersConfig.setMasterConnectionPoolSize(configuration.getMaxTotal());
                clusterServersConfig.setMasterConnectionMinimumIdleSize(configuration.getMinIdle());
                clusterServersConfig.setSlaveConnectionPoolSize(configuration.getMaxTotal());
                clusterServersConfig.setSlaveConnectionMinimumIdleSize(configuration.getMinIdle());
                clusterServersConfig.setConnectTimeout(configuration.getTimeout());
                clusterServersConfig.setRetryInterval(configuration.getRetryInterval());
                clusterServersConfig.setTimeout(configuration.getTimeout());
                if (StringUtils.isNotEmpty(configuration.getPassword())) {
                    clusterServersConfig.setPassword(configuration.getPassword());
                }
                for (RedisServer server : servers) {
                	String address = server.toString();
                	if(!address.startsWith("redis://")) {
                		address = "redis://"+address;
                	}
                	  logger.error("redissonConfig model  "+ configuration.getModel() + " address : "+address);
                    clusterServersConfig.addNodeAddress(address);
                }

            } else if (configuration.getModel() ==2 ) {
                SingleServerConfig singleServerConfig = redissonConfig.useSingleServer();
                singleServerConfig.setSubscriptionsPerConnection(configuration.getMaxTotal());
                singleServerConfig.setConnectionPoolSize(configuration.getMaxTotal());
                singleServerConfig.setConnectionMinimumIdleSize(configuration.getMinIdle());
                singleServerConfig.setConnectTimeout(configuration.getTimeout());
                singleServerConfig.setRetryInterval(configuration.getRetryInterval());
                singleServerConfig.setTimeout(configuration.getTimeout());
                String address = servers.get(0).toString();
            	if(!address.startsWith("redis://")) {
            		address = "redis://"+address;
            	}
                singleServerConfig.setAddress(address);
                logger.error("redissonConfig model  "+ configuration.getModel() + " address : "+address);
                if (StringUtils.isNotEmpty(configuration.getPassword())) {
                    singleServerConfig.setPassword(configuration.getPassword());
                }
            }

            Serializer serializer = configuration.getSerializer();
            Deserializer deserializer = configuration.getDeserializer();
            if (serializer != null || deserializer != null) {
                RedissonCodec codec = new RedissonCodec(serializer, deserializer);
                redissonConfig.setCodec(codec);
            }
            logger.error("redissonConfig : "+JSON.toJSONString(redissonConfig));
            redissonClient = Redisson.create(redissonConfig);
        }

        config.setMaxTotal(getConfiguration().getMaxTotal());
        config.setMaxIdle(getConfiguration().getMaxIdle());
        config.setMinIdle(getConfiguration().getMinIdle());
        config.setMaxWaitMillis(getConfiguration().getTimeout());
        config.setTestOnBorrow(false);
        config.setTestOnReturn(false);
        config.setTestWhileIdle(true);
        config.setTimeBetweenEvictionRunsMillis(configuration.getTimeBetweenEvict());
        config.setMinEvictableIdleTimeMillis(configuration.getTimeBetweenEvict());
    }

    public Connection createConnection(){
        if (closed){
            throw new RedisClientException("connection factory has been closed");
        }
        return makeConnection(redissonClient);
    }

    public RedisConfiguration getConfiguration() {
        return configuration;
    }

    protected abstract Connection makeConnection(RedissonClient redissonClient);

    @Override
    public void close() {
        closed = true;
        if (redissonClient != null && !redissonClient.isShutdown()){
            redissonClient.shutdown();
        }
    }

}
