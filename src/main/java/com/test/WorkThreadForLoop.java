package com.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

 

import com.json.redis.RedisTemplate;

 

public class WorkThreadForLoop extends Thread {

	private static Random r = new Random();


	RedisTemplate redisTemplate;

	public WorkThreadForLoop(RedisTemplate redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	public void run() {
		int count = 0;
		for (int i = 0; i < 10000; i++) {
			long start = System.currentTimeMillis();
			count++;

			int notHitCount = getForLoopRandomSkus();
			long cost = System.currentTimeMillis() - start;
			// System.out.println(" notHitCount "+notHitCount +", cost "+cost);
			if (count % 10 == 0) {
				System.out.println("WorkThreadForLoop notHitCount " + notHitCount + ", cost "
						+ cost);
			}
			if (cost > 2000) {
				System.out
						.println("WorkThreadForLoop******************************************** notHitCount "
								+ notHitCount + ", cost " + cost);
			}
			//
		}
	}

	public static List<String> getRandom20keys() {
		List<String> keys = new ArrayList<String>();
		for (int i = 0; i < 20; i++) {
			int randomValue = r.nextInt(10000);
			keys.add(Constant.newKeyPrefix + randomValue);
		}
		return keys;
	}

	public int getForLoopRandomSkus() {
		int notHitCount = 0;
		List<String> keys = getRandom20keys();
		for (String item : keys) {
			Sku Sku = redisTemplate.get(item, Sku.class);
			if (Sku == null) {
				notHitCount++;
				Sku = new Sku();
				Sku.setSku(item);
				Sku.setBarcode("1212454579854_" + item);
				redisTemplate.set(item, Sku, Constant.DAY);
			}
		}
		return notHitCount;
	}


}
