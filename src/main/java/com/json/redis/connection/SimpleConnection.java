package com.json.redis.connection;

import org.redisson.api.RedissonClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by   on 12/22/16.
 */
public class SimpleConnection extends AbstractConnection implements Connection {

    private Jedis jedis;

    public SimpleConnection(Jedis jedis, RedissonClient redissonClient) {
        super(redissonClient);
        this.jedis = jedis;
    }

    @Override
    public void set(byte[] key, byte[] value) {
        jedis.set(key, value);
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        return jedis.setnx(key, value);
    }

    @Override
    public void setex(byte[] key, int seconds, byte[] value) {
        jedis.setex(key, seconds, value);
    }

    @Override
    public byte[] get(byte[] key) {
        return jedis.get(key);
    }

    @Override
    public void mset(String... keyValue) {
        jedis.mset(keyValue);
    }

    @Override
    public void msetex(int seconds, String... keyValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mset(Map<String, Object> keyValues) {
        if (keyValues == null || keyValues.size() == 0) {
            return;
        }

        String[] kvs = new String[keyValues.size() * 2];
        int i = 0;
        for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
            kvs[i] = entry.getKey();
            Object value = entry.getValue();
            //判断value是否String类型,如果不是抛异常
            if (value == null || !String.class.equals(value.getClass())) {
                throw new UnsupportedOperationException("redis simpleConnection support map value type only String");
            }
            kvs[i + 1] = value.toString();
            i += 2;
        }

        jedis.mset(kvs);
    }

    @Override
    public void msetex(int seconds, Map<String, Object> keyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> mget(String... keys) {
        return jedis.mget(keys);
    }

    @Override
    public List<Object> mget(Object... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long incr(byte[] key) {
        return jedis.incr(key);
    }

    @Override
    public Long incrBy(byte[] key, long increment) {
        return jedis.incrBy(key, increment);
    }

    @Override
    public Double incrByFloat(byte[] key, double increment) {
        return jedis.incrByFloat(key, increment);
    }

    @Override
    public Long decr(byte[] key) {
        return jedis.decr(key);
    }

    @Override
    public Long decrBy(byte[] key, long decrement) {
        return jedis.decrBy(key, decrement);
    }

    @Override
    public Long del(byte[] key) {
        return jedis.del(key);
    }

    @Override
    public void expire(byte[] key, int seconds) {
        jedis.expire(key, seconds);
    }

    @Override
    public boolean exists(byte[] key) {
        return jedis.exists(key);
    }

    @Override
    public Long ttl(byte[] key) {
        return jedis.ttl(key);
    }

    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        return jedis.setbit(key, offset, value);
    }

    @Override
    public Boolean getbit(byte[] key, long offset) {
        return jedis.getbit(key, offset);
    }

    @Override
    public Long bitcount(byte[] key) {
        return jedis.bitcount(key);
    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        return bitcount(key, start, end);
    }

    @Override
    public byte[] lindex(byte[] key, long index) {
        return jedis.lindex(key, index);
    }

    @Override
    public Long llen(byte[] key) {
        return jedis.llen(key);
    }

    @Override
    public Long lpush(byte[] key, byte[]... value) {
        return jedis.lpush(key, value);
    }

    @Override
    public Long rpush(byte[] key, byte[]... value) {
        return jedis.rpush(key, value);
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long stop) {
        return jedis.lrange(key, start, stop);
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        return jedis.lrem(key, count, value);
    }

    @Override
    public void lset(byte[] key, long index, byte[] value) {
        jedis.lset(key, index, value);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return jedis.hget(key, field);
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return jedis.hset(key, field, value);
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return jedis.hsetnx(key, field, value);
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return jedis.hmset(key, hash);
    }

    @Override
    public Collection<byte[]> hval(byte[] key) {
        return jedis.hvals(key);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return jedis.hgetAll(key);
    }

    @Override
    public Long hdel(byte[] key, byte[] field) {
        return jedis.hdel(key, field);
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        return jedis.hkeys(key);
    }

    @Override
    public Long hlen(byte[] key) {
        return jedis.hlen(key);
    }

    @Override
    public boolean hexists(byte[] key, byte[] field) {
        return jedis.hexists(key, field);
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long increment) {
        return jedis.hincrBy(key, field, increment);
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double increment) {
        return jedis.hincrByFloat(key, field, increment);
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return jedis.hmget(key, fields);
    }

    @Override
    public Long sadd(byte[] key, byte[] value) {
        return jedis.sadd(key, value);
    }

    @Override
    public Long scard(byte[] key) {
        return jedis.scard(key);
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        return jedis.smembers(key);
    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        return jedis.sismember(key, member);
    }

    @Override
    public byte[] spop(byte[] key) {
        return jedis.spop(key);
    }

    @Override
    public Set<byte[]> spop(byte[] key, int count) {
        return jedis.spop(key, count);
    }

    @Override
    public byte[] srandommember(byte[] key) {
        return jedis.srandmember(key);
    }

    @Override
    public List<byte[]> srandommember(byte[] key, int count) {
        return jedis.srandmember(key, count);
    }

    @Override
    public Long srem(byte[] key, byte[]... member) {
        return jedis.srem(key, member);
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        return jedis.sunion(keys);
    }

    @Override
    public Long sunionstore(byte[] destination, byte[]... keys) {
        return jedis.sunionstore(destination, keys);
    }

    @Override
    public Long smove(byte[] sourceKey, byte[] destKey, byte[] member) {
        return jedis.smove(sourceKey, destKey, member);
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        return jedis.sdiff(keys);
    }

    @Override
    public Long sdiffstore(byte[] destination, byte[]... keys) {
        return jedis.sdiffstore(destination, keys);
    }

    @Override
    public Set<byte[]> sinter(byte[] keys) {
        return jedis.sinter(keys);
    }

    @Override
    public Long sinterstore(byte[] destination, byte[]... keys) {
        return jedis.sinterstore(destination, keys);
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        return jedis.zadd(key, scoreMembers);
    }

    @Override
    public Long zcard(byte[] key) {
        return jedis.zcard(key);
    }

    @Override
    public long zcount(byte[] key, double min, double max) {
        return jedis.zcount(key, min, max);
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        return jedis.zincrby(key, score, member);
    }

    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        return jedis.zrange(key, start, end);
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        return jedis.zrevrange(key, start, end);
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        return jedis.zrangeWithScores(key, start, end);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        return jedis.zrevrangeWithScores(key, start, end);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return jedis.zrangeByScore(key, min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return jedis.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return jedis.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return jedis.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Double zscore(byte[] key, byte[] member) {
        return jedis.zscore(key, member);
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        return jedis.zrevrank(key, member);
    }

    @Override
    public Long zrem(String key, String... members) {
        return jedis.zrem(key, members);
    }
}
