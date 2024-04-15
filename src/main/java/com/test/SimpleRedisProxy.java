package com.test;

import com.json.redis.RedisTemplate;
import com.json.redis.conf.RedisClusterConfiguration;
import com.json.redis.conf.RedisSingleConfiguration;

public class SimpleRedisProxy {

 
	public static RedisTemplate getRedisTemplate() {
		String ips = "192.168.0.107:6379";
		//RedisClusterConfiguration redisConfiguration = new RedisClusterConfiguration();
		RedisSingleConfiguration redisConfiguration = new RedisSingleConfiguration();
		redisConfiguration.setIpAddressList(ips);
		redisConfiguration.setLockIpAddressList(ips);
		redisConfiguration.setMaxIdle(2);
		redisConfiguration.setMaxTotal(10);
		redisConfiguration.setMinIdle(2);
		redisConfiguration.setTimeout(3000);
		redisConfiguration.setBatchModel(1);
		redisConfiguration.setModel(2);
		//redisConfiguration.setPassword("123456");
		com.json.redis.serializer.codec.AbstractCodec redisValueCodec = new com.json.redis.serializer.codec.StringCodec();
		RedisTemplate redisTemplate = new RedisTemplate();
		redisTemplate.setNamespace("json");
		redisTemplate.setDeserializer(redisValueCodec);
		redisTemplate.setSerializer(redisValueCodec);
		redisTemplate.setRedisConfiguration(redisConfiguration);
		redisTemplate.init();
		return redisTemplate;
	}


	
}
