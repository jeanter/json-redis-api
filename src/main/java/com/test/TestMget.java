package com.test;

import com.json.redis.RedisTemplate;



public class TestMget {

	public final static Integer DAY = 60 * 60 * 24;

	public static void main(String[] args) {
		RedisTemplate redisTemplate = SimpleRedisProxy.getRedisTemplate();
		for (int i = 0; i < 15; i++) {
			WorkThreadForMget thread = new WorkThreadForMget(redisTemplate);
			//每个线程独占redisTemplate实例，毛刺现象消失，基本判断是内部锁机制引起的等待
			//WorkThreadForMget thread = new WorkThreadForMget(SimpleRedisProxy.getRedisTemplate());
			thread.start();
		}
		
	}

}
