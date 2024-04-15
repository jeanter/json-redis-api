package com.test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.collections.CollectionUtils;

import com.json.redis.RedisTemplate;
import com.json.redis.conf.RedisClusterConfiguration;

public class RedisProxy {

	private static Random r = new Random();
	
	public final static Integer DAY = 60 * 60 * 24;

	static long waitTime = 15;

	static String newKeyPrefix = "abc_v_";

	RedisTemplate redisTemplate;

	public RedisProxy() {
		redisTemplate = getRedisTemplate();
	}

	public RedisTemplate getRedisTemplate() {
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
		return redisTemplate;
	}

	public <T> List<T> mget(Class<T> clazz, String... keys) {
		return redisTemplate.mget((Type) clazz, keys);
	}

	public void msetex(Map<String, Object> keyValues, int seconds) {
		redisTemplate.msetex(keyValues, seconds);
	}

	public <T> T get(String key, Class<T> clazz) {
		return redisTemplate.get(key, (Type) clazz);
	}

    public void set(String key, Object value, int seconds) {
    	redisTemplate.set(key, value, seconds);
    }
    
	public Map<String, RedisObject> getCacheMap(List<String> params) {

		if (CollectionUtils.isNotEmpty(params)) {
			List<String> keys = new ArrayList<String>();
			for (String param : params) {
				keys.add(newKeyPrefix + param);
			}
			List<RedisObject> cacheList = mget(RedisObject.class,
					keys.toArray(new String[keys.size()]));

			Map<String, RedisObject> cacheMap = new HashMap<String, RedisObject>();
			if (CollectionUtils.isNotEmpty(cacheList)) {
				for (RedisObject cache : cacheList) {
					if (cache != null) {
						cacheMap.put(cache.getCacheKey(), cache);
					}
				}
			}
			return cacheMap;
		}
		return new HashMap<String, RedisObject>();
	}
	
	public Map<String, RedisObject> getCacheMapForLoop(List<String> params) {
		Map<String, RedisObject> cacheMap = new HashMap<String, RedisObject>();
		List<String> needQueryParams = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(params)) {
			for (String param : params) {
				RedisObject cacheObject = (RedisObject) redisTemplate.get(newKeyPrefix + param, RedisObject.class);
				  if(cacheObject == null){
					  needQueryParams.add(param);
					  continue;
				  }
			 
				  cacheMap.put(cacheObject.getCacheKey(), cacheObject);
			}
		}
		if (CollectionUtils.isNotEmpty(needQueryParams)) {
			List<RedisObject> cacheList = waitAndReturn(needQueryParams);
			if (CollectionUtils.isNotEmpty(cacheList)) {
				for(RedisObject item : cacheList){
					redisTemplate.set(item.getCacheKey(), item, DAY);
				}
			}
		}
		return cacheMap;
	}

	public  Map<String, RedisObject> getAndSetCaches(List<String> keys) {

		List<String> needQueryParams = new ArrayList<>();
		List<Object> result = new ArrayList<>();

		Map<String, RedisObject> redisMap = this.getCacheMap(keys);
		// for循环，从redis中获取数据.
		for (String param : keys) {
			CacheObject<RedisObject> cacheObject = null;
			RedisObject redisObj = redisMap.get(newKeyPrefix + param);
			if (redisObj != null && redisObj.getCacheValue() != null) {
				cacheObject = (CacheObject) redisObj.getCacheValue();
			}
			if (cacheObject == null) {// redis未获取到数据，临时存储起来，便于后面查询数据使用.
				needQueryParams.add(param);
			} else {
				
				result.add(cacheObject.getObject());
			}
		}

		if (CollectionUtils.isNotEmpty(needQueryParams)) {
			List<RedisObject> cacheList = waitAndReturn(needQueryParams);
			if (CollectionUtils.isNotEmpty(cacheList)) {
				mapSetRedisObject(cacheList,DAY);
			}
		}
		
		return redisMap;
	}

	// 模拟拉取数据 等等50ms
	private List<RedisObject> waitAndReturn(List<String> needQueryParams) {
		try {
			List<RedisObject> cacheList = getCacheList(getRandom20SkuList(needQueryParams));
			Thread.sleep(waitTime);
			return cacheList;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public void mapSetRedisObject(List<RedisObject> cacheList, int expire) {
		if (CollectionUtils.isNotEmpty(cacheList)) {
			try {
				Map<String, Object> kvMap = new HashMap<String, Object>();
				for (RedisObject cache : cacheList) {
					kvMap.put(cache.getCacheKey(), cache);
				}
				msetex(kvMap, expire);
			} catch (Exception e) {

				System.out.println("error.....................");
			}
		}
	}

	public Map<String, Object> getRedisValueMap(List<Sku> skuList) {
		long currentTime = System.currentTimeMillis();
		List<RedisObject> cacheList = new ArrayList<RedisObject>();
		for (Sku Sku : skuList) {
			CacheObject<Sku> cacheObject = new CacheObject<Sku>(Sku,
					currentTime);
			cacheList.add(new RedisObject(newKeyPrefix + Sku.getSku(),
					cacheObject));
		}
		Map<String, Object> kvMap = new HashMap<String, Object>();
		for (RedisObject cache : cacheList) {
			kvMap.put(cache.getCacheKey(), cache);
		}
		return kvMap;
	}

	public List<Sku> getRandom20SkuList() {
		List<Sku> skuList = new ArrayList<Sku>();
		for (int i = 0; i < 20; i++) {
			Sku Sku = new Sku();
			int randomValue = r.nextInt(1500000);
			Sku.setId(1225l + randomValue);
			Sku.setSku("sdjfksdjf8s78ds_" + randomValue);
			Sku.setBarcode("1212454579854_" + randomValue);
			skuList.add(Sku);
		}
		return skuList;
	}

	public List<Sku> getRandom20SkuList(List<String> needQueryParams) {
		List<Sku> skuList = new ArrayList<Sku>();
		if (needQueryParams == null || needQueryParams.size() < 1) {
			return skuList;
		}
		for (String sku : needQueryParams) {
			Sku Sku = new Sku();
			int randomValue = r.nextInt(1500000);
			Sku.setId(1225l + randomValue);
			Sku.setSku(sku);
			Sku.setBarcode("1212454579854_" + randomValue);
			skuList.add(Sku);
		}
		return skuList;
	}

	public List<RedisObject> getCacheList(List<Sku> skuList) {
		long currentTime = System.currentTimeMillis();
		List<RedisObject> cacheList = new ArrayList<RedisObject>();
		for (Sku Sku : skuList) {
			CacheObject<Sku> cacheObject = new CacheObject<Sku>(Sku,
					currentTime);
			cacheList.add(new RedisObject(newKeyPrefix + Sku.getSku(),
					cacheObject));
		}
		return cacheList;
	}

	public List<String> getRediskeys(List<Sku> skuList) {
		List<String> keys = new ArrayList<String>();
		for (Sku sku : skuList) {
			keys.add(sku.getSku());
		}
		return keys;
	}

	public static List<String> getRandom20keys() {
		List<String> keys = new ArrayList<String>();
		for (int i = 0; i < 20; i++) {
			int randomValue = r.nextInt(1500000);
			keys.add("sdjfksdjf8s78ds_" + randomValue);
		}
		return keys;
	}

}
