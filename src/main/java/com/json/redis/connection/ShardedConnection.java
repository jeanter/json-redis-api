package com.json.redis.connection;

import org.redisson.api.RedissonClient;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by   on 12/21/16.
 */
public class ShardedConnection extends AbstractConnection implements Connection {

    private ShardedJedis shardedJedis;

    public ShardedConnection(ShardedJedis shardedJedis, RedissonClient redissonClient) {
        super(redissonClient);
        this.shardedJedis = shardedJedis;

    }

    @Override
    public void set(byte[] key, byte[] value) {
        shardedJedis.set(key, value);
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        return shardedJedis.setnx(key, value);
    }

    @Override
    public void setex(byte[] key, int seconds, byte[] value) {
        shardedJedis.setex(key, seconds, value);
    }

    @Override
    public byte[] get(byte[] key) {
        return shardedJedis.get(key);
    }

    @Override
    public List<String> mget(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> mget(Object... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long incr(byte[] key) {
        return shardedJedis.incr(key);
    }

    @Override
    public Long incrBy(byte[] key, long increment) {
        return shardedJedis.incrBy(key, increment);
    }

    @Override
    public Long decr(byte[] key) {
        return shardedJedis.decr(key);
    }

    @Override
    public Long decrBy(byte[] key, long decrement) {
        return shardedJedis.decrBy(key, decrement);
    }

    @Override
    public Double incrByFloat(byte[] key, double increment) {
        return shardedJedis.incrByFloat(key, increment);
    }

    @Override
    public void mset(String... keyValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void msetex(int seconds, String... keyValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mset(Map<String, Object> keyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void msetex(int seconds, Map<String, Object> keyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long del(byte[] key) {
        return shardedJedis.del(key);
    }

    @Override
    public void expire(byte[] key, int seconds) {
        shardedJedis.expire(key, seconds);
    }

    @Override
    public boolean exists(byte[] key) {
        return shardedJedis.exists(key);
    }

    @Override
    public Long ttl(byte[] key) {
        return shardedJedis.ttl(key);
    }

    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        return shardedJedis.setbit(key, offset, value);
    }

    @Override
    public Boolean getbit(byte[] key, long offset) {
        return shardedJedis.getbit(key, offset);
    }

    @Override
    public Long bitcount(byte[] key) {
        return shardedJedis.bitcount(key);
    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        return shardedJedis.bitcount(key, start, end);
    }

    @Override
    public byte[] lindex(byte[] key, long index) {
        return shardedJedis.lindex(key, index);
    }

    @Override
    public Long llen(byte[] key) {
        return shardedJedis.llen(key);
    }

    @Override
    public Long lpush(byte[] key, byte[]... value) {
        return shardedJedis.lpush(key, value);
    }

    @Override
    public Long rpush(byte[] key, byte[]... value) {
        return shardedJedis.rpush(key, value);
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long stop) {
        return shardedJedis.lrange(key, start, stop);
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        return shardedJedis.lrem(key, count, value);
    }

    @Override
    public void lset(byte[] key, long index, byte[] value) {
        shardedJedis.lset(key, index, value);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return shardedJedis.hget(key, field);
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return shardedJedis.hset(key, field, value);
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return shardedJedis.hsetnx(key, field, value);
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return shardedJedis.hmset(key, hash);
    }

    @Override
    public Collection<byte[]> hval(byte[] key) {
        return shardedJedis.hvals(key);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return shardedJedis.hgetAll(key);
    }

    @Override
    public Long hdel(byte[] key, byte[] field) {
        return shardedJedis.hdel(key, field);
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        return shardedJedis.hkeys(key);
    }

    @Override
    public Long hlen(byte[] key) {
        return shardedJedis.hlen(key);
    }

    @Override
    public boolean hexists(byte[] key, byte[] field) {
        return false;
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long increment) {
        return shardedJedis.hincrBy(key, field, increment);
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double increment) {
        return shardedJedis.hincrByFloat(key, field, increment);
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return shardedJedis.hmget(key, fields);
    }

    @Override
    public Long sadd(byte[] key, byte[] value) {
        return shardedJedis.sadd(key, value);
    }

    @Override
    public Long scard(byte[] key) {
        return shardedJedis.scard(key);
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        return shardedJedis.smembers(key);
    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        return shardedJedis.sismember(key, member);
    }

    @Override
    public byte[] spop(byte[] key) {
        return shardedJedis.spop(key);
    }

    @Override
    public Set<byte[]> spop(byte[] key, int count) {
        return shardedJedis.spop(key, count);
    }

    @Override
    public byte[] srandommember(byte[] key) {
        return shardedJedis.srandmember(key);
    }

    @Override
    public List<byte[]> srandommember(byte[] key, int count) {
        return shardedJedis.srandmember(key, count);
    }

    @Override
    public Long srem(byte[] key, byte[]... member) {
        return shardedJedis.srem(key, member);
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sunionstore(byte[] destination, byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long smove(byte[] sourceKey, byte[] destKey, byte[] member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sdiffstore(byte[] destination, byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<byte[]> sinter(byte[] keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sinterstore(byte[] destination, byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    public void close() {
        if (shardedJedis != null) {
            shardedJedis.close();
        }
    }


    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        return shardedJedis.zadd(key, scoreMembers);
    }

    @Override
    public Long zcard(byte[] key) {
        return shardedJedis.zcard(key);
    }

    @Override
    public long zcount(byte[] key, double min, double max) {
        return shardedJedis.zcount(key, min, max);
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        return shardedJedis.zincrby(key, score, member);
    }

    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        return shardedJedis.zrange(key, start, end);
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        return shardedJedis.zrevrange(key, start, end);
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        return shardedJedis.zrangeWithScores(key, start, end);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        return shardedJedis.zrevrangeWithScores(key, start, end);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return shardedJedis.zrangeByScore(key, min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return shardedJedis.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return shardedJedis.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return shardedJedis.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return shardedJedis.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return shardedJedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Double zscore(byte[] key, byte[] member) {
        return shardedJedis.zscore(key, member);
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        return shardedJedis.zrevrank(key, member);
    }

    @Override
    public Long zrem(String key, String... members) {
        return shardedJedis.zrem(key, members);
    }
}
