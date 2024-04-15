package com.test;

import com.json.redis.RedisTemplate;

public class TestLoop {

	public final static Integer DAY = 60 * 60 * 24;

	public static void main(String[] args) {
		RedisTemplate redisTemplate = SimpleRedisProxy.getRedisTemplate();
		for (int i = 0; i < 15; i++) {
			WorkThreadForLoop thread = new WorkThreadForLoop(redisTemplate);
			thread.start();
		}
	}

}
