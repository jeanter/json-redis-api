package com.test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.json.redis.RedisTemplate;
import com.json.redis.conf.RedisClusterConfiguration;

public class TestMain {

	public static void main(String[] args) {
		RedisClusterConfiguration redisConfiguration = new RedisClusterConfiguration();
		redisConfiguration
				.setIpAddressList("10.88.17.119:6379,10.88.17.109:6379,10.88.17.99:6379,10.88.17.89:6379,10.88.17.79:6379,10.88.17.69:6379");
		redisConfiguration
				.setLockIpAddressList("10.88.17.119:6379,10.88.17.109:6379,10.88.17.99:6379,10.88.17.89:6379,10.88.17.79:6379,10.88.17.69:6379");
		redisConfiguration.setMaxIdle(2);
		redisConfiguration.setMaxTotal(10);
		redisConfiguration.setMinIdle(2);
		redisConfiguration.setTimeout(3000);

		com.json.redis.serializer.codec.SerializationCodec redisValueCodec = new com.json.redis.serializer.codec.SerializationCodec();

		RedisTemplate redisTemplate = new RedisTemplate();
		redisTemplate.setNamespace("");
		redisTemplate.setDeserializer(redisValueCodec);
		redisTemplate.setSerializer(redisValueCodec);
		redisTemplate.setRedisConfiguration(redisConfiguration);
		redisTemplate.init();

		long currentTime = System.currentTimeMillis();

		
		List<RedisObject> cacheList = new ArrayList<RedisObject>();

		String newKeyPrefix = "abc_v_";

		List<Sku> skuList = new ArrayList<Sku>();
		for (int i = 0; i < 20; i++) {
			Sku Sku = new Sku();
			Sku.setId(1225l);
			Sku.setSku("sdjfksdjf8s78ds_" + i);
			Sku.setBarcode("1212454579854_" + i);
			skuList.add(Sku);
		
		}
		for(Sku Sku  :skuList){
			CacheObject<Sku> cacheObject = new CacheObject<Sku>(Sku,
					currentTime);
			cacheList.add(new RedisObject(newKeyPrefix + Sku.getSku(),
					cacheObject));
		}
		Map<String, Object> kvMap = new HashMap<String, Object>();
		for (RedisObject cache : cacheList) {
			kvMap.put(cache.getCacheKey(), cache);
		}
		redisTemplate.msetex(kvMap, 30 * 60);

		
		List<String> keys = new ArrayList<String>();
		for (Sku sku : skuList) {
			keys.add(newKeyPrefix + sku.getSku());
		}
		
		List<RedisObject> redis_cacheList = redisTemplate.mget(RedisObject.class, keys.toArray(new String[keys.size()]));
		
		for(RedisObject item : redis_cacheList){
			CacheObject<Sku> cacheObject = (CacheObject<Sku>) item.getCacheValue();
			System.out.println(cacheObject.getObject().getBarcode());
		}
		System.out.println(redis_cacheList.size());
	}

}
