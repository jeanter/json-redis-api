package com.test;

import com.json.redis.RedisTemplate;
import com.json.redis.conf.RedisClusterConfiguration;
import com.json.redis.conf.RedisShardedConfiguration;
import com.json.redis.conf.RedisSingleConfiguration;

public class Test {

	public final static String SSO_SESSION_OBJ_PREFIX = "sso_session_obj_";

	public final static String SSO_USER_PRIVATE_KEY_PREFIX = "sso_session_user_private_keys_";

	public final static String SSO_TOKEN_PREFIX = "sso_token_";

	public static RedisTemplate redisTemplate;

	public static void setSelf(RedisTemplate input_redisTemplate) {
		redisTemplate = input_redisTemplate;
	}

 

	public static boolean overload(String key, int seconds, int maxNum) {
		try {
			Long count = redisTemplate.getLong(key);
			// 过期 或者第一次获取该key
			if (count == null) {
				count = redisTemplate.incr(key);
				redisTemplate.expire(key, seconds);
				// 小于阈值 +1
			} else if (count <= maxNum) {
				count = redisTemplate.incr(key);
				// 大于阈值 不再incr
			} else {
				// do nothing 
			}
			return count <= maxNum;
		} catch (Exception e) {
			e.printStackTrace();
		}
		// redis 异常 直接通过
		return true;
	}

	public static void main(String[] args) throws InterruptedException {
	    String key = "key_"; 
	    String value = "key_"; 
		RedisTemplate redisTemplate = SimpleRedisProxy.getRedisTemplate();
		setSelf(redisTemplate);
		int count = 10000;
		long start = System.currentTimeMillis();
		for(int i = 0 ; i< count; i++ ) {
			if(i ==1) {
				System.out.println("start................... "+ (System.currentTimeMillis() -start));	
			}
			redisTemplate.set(key +i, value +"testasefds"+i);
		}
	 
		System.out.println("cost "+ (System.currentTimeMillis() -start));

	}

}
