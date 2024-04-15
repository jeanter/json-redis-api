package com.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

 

import com.json.redis.RedisTemplate;

 

public class WorkThreadForMget extends Thread {

	private static Random r = new Random();


	RedisTemplate redisTemplate;

	public WorkThreadForMget(RedisTemplate redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	public void run() {
		int count = 0;
		for (int i = 0; i < 10000; i++) {
			long start = System.currentTimeMillis();
			count++;
			int notHitCount = mgetRandomSkus();
			long cost = System.currentTimeMillis() - start;
			// System.out.println(" notHitCount "+notHitCount +", cost "+cost);
			if (count % 10 == 0) {
				System.out.println("WorkThreadForMget notHitCount " + notHitCount + ", cost "
						+ cost);
			}
			if (cost > 2000) {
				System.out
						.println("WorkThreadForMget ******************************************** notHitCount "
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


	public int mgetRandomSkus() {
		int notHitCount = 0;
		List<String> keys = getRandom20keys();
		List<Sku> cacheList = redisTemplate.mget(Sku.class,
				keys.toArray(new String[keys.size()]));

		Map<String, Sku> map = new HashMap<String, Sku>();
		for (Sku Sku : cacheList) {
			if (Sku != null) {
				map.put(Sku.getSku(), Sku);
			}
		}
		List<Sku> needCacheList = new ArrayList<Sku>();
		for (String item : keys) {
			if (!map.containsKey(item)) {
				{
					notHitCount++;
					Sku Sku = new Sku();
					Sku.setSku(item);
					Sku.setBarcode("1212454579854_" + item);
					needCacheList.add(Sku);
				}

			}
		}
		Map<String, Object> kvMap = new HashMap<String, Object>();
		for (Sku Sku : needCacheList) {
			kvMap.put(Sku.getSku(), Sku);
		}
		if (kvMap.size() > 0) {
			redisTemplate.msetex(kvMap, Constant.DAY);
		}

		return notHitCount;
	}

}
