package com.json.redis.connection;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.concurrent.TimeUnit;

/**
 * Created by   on 12/24/16.
 */
public abstract class AbstractConnection implements Connection {

    private RedissonClient redissonClient;
    private RLock rLock;

    public AbstractConnection(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Override
    public boolean tryLock(String lock, int maxWait, int maxLockTimeAfterSuccess, TimeUnit timeUnit) {
        if (redissonClient == null) {
            return false;
        }
        try {
            rLock = redissonClient.getLock(lock);
            return rLock.tryLock(maxWait, maxLockTimeAfterSuccess, timeUnit);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void unlock() {
        if (rLock != null) {
            rLock.unlock();
        }
    }

    @Override
    public void forceUnlock() {
        if (rLock != null) {
            rLock.forceUnlock();
        }
    }

    @Override
    public boolean isLocked() {
        return rLock != null && rLock.isLocked();
    }

    public RedissonClient getRedissonClient() {
        return redissonClient;
    }
}
