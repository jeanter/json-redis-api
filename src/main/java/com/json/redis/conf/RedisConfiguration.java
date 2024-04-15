package com.json.redis.conf;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.json.redis.serializer.Deserializer;
import com.json.redis.serializer.Serializer;

/**
 * Created by   on 12/21/16.
 */
public  class RedisConfiguration implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(RedisConfiguration.class);

    private String ipAddressList;
    private String lockIpAddressList;
    private int maxTotal = 8;
    private int maxIdle = 8;
    private int minIdle = 1;
    private int timeout = 3 * 1000;
    private int timeBetweenEvict = 60 * 1000;
    //use for redission
    private int retryInterval = 1000;
    private Serializer serializer;
    private Deserializer deserializer;
    //support auth
    private String password;
    private int soTimeout = 1000;
    private int maxAttempts = 3;
    //cluster mget mset 操作模式
    private int batchModel = 0;

    //全部对外配置是集群，内部再根据该配置来判断是普通主从还是集群 0是集群    1是分片  2 是单节点     主要是为了适配现在的配置不改的情况下 再加一个配置来判断是否是集群还是主从
    private int model = 0;

    public String getIpAddressList() {
        return ipAddressList;
    }

    public void setIpAddressList(String ipAddressList) {
        this.ipAddressList = ipAddressList;
    }

    public String getLockIpAddressList() {
        return lockIpAddressList;
    }

    public void setLockIpAddressList(String lockIpAddressList) {
        this.lockIpAddressList = lockIpAddressList;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }


    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getTimeBetweenEvict() {
        return timeBetweenEvict;
    }

    public void setTimeBetweenEvict(int timeBetweenEvict) {
        this.timeBetweenEvict = timeBetweenEvict;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public Deserializer getDeserializer() {
        return deserializer;
    }

    public void setDeserializer(Deserializer deserializer) {
        this.deserializer = deserializer;
    }

    public String getPassword() {
    	if(StringUtils.isNotBlank(password)) {
    		return password;
    	}
        return System.getProperty("redis_password");
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public int getBatchModel() {
        return batchModel;
    }

    public void setBatchModel(int batchModel) {
        this.batchModel = batchModel;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

	public int getModel() {
//        // 测试环境是主从
//		if(StringUtils.isNotBlank(System.getProperty("redis_env")) && "sit".equals(System.getProperty("redis_env"))) {
//			this.model = 1;
//		}
//        logger.error("redis model:{},env:{}", model, System.getProperty("redis_env"));
		return model;
	}

	public void setModel(int model) {
		this.model = model;
	}
    
    
}
