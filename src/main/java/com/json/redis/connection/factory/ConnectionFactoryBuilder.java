package com.json.redis.connection.factory;

import com.json.redis.conf.RedisClusterConfiguration;
import com.json.redis.conf.RedisConfiguration;
import com.json.redis.conf.RedisShardedConfiguration;
import com.json.redis.conf.RedisSingleConfiguration;

/**
 * Created by   on 12/21/16.
 */
public class ConnectionFactoryBuilder {

    private RedisConfiguration configuration;

    public ConnectionFactoryBuilder(){

    }

    public ConnectionFactoryBuilder setConfiguration(RedisConfiguration configuration){
        this.configuration = configuration;
        return this;
    }

    public ConnectionFactory build(){
        if (configuration.getModel() ==1) {
            return new ShardedConnectionFactory( configuration);
        }else if ( configuration.getModel() ==0 ) {
            return new ClusterConnectionFactory(configuration);
        }else if(configuration.getModel() == 2){
            return new SimpleConnectionFactory( configuration);
        }

        return null;
    }
}
