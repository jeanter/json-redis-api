package com.test;


import java.io.Serializable;

public class RedisObject implements Serializable {
	private static final long serialVersionUID = -4717281890867589469L;
	private String cacheKey;
	private Object cacheValue;
	
	public RedisObject() {}
	
	public RedisObject(String cacheKey,Object cacheValue) {
		this.cacheKey = cacheKey;
		this.cacheValue = cacheValue;
	}

	public String getCacheKey() {
		return cacheKey;
	}

	public void setCacheKey(String cacheKey) {
		this.cacheKey = cacheKey;
	}

	public Object getCacheValue() {
		return cacheValue;
	}

	public void setCacheValue(Object cacheValue) {
		this.cacheValue = cacheValue;
	}

}
