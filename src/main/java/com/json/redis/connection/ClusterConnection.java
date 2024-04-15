package com.json.redis.connection;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.json.redis.BatchModel;
import com.json.redis.exception.RedisClientException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.cluster.ClusterConnectionManager;
import org.redisson.connection.MasterSlaveEntry;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.sortedset.ZAddParams;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by   on 12/21/16.
 */
public class ClusterConnection extends AbstractConnection implements Connection {

    private JedisCluster cluster;
    private BatchModel batchModel;
    private Encoder serializer;

    private Decoder deserializer;

    public ClusterConnection(JedisCluster cluster, RedissonClient redissonClient) {
        super(redissonClient);
        this.cluster = cluster;
        this.batchModel = BatchModel.ITERATION;
        this.serializer = redissonClient.getConfig().getCodec().getValueEncoder();
        this.deserializer = redissonClient.getConfig().getCodec().getValueDecoder();
    }

    public ClusterConnection(JedisCluster cluster, RedissonClient redissonClient, BatchModel batchModel) {
        super(redissonClient);
        this.cluster = cluster;
        this.batchModel = batchModel;
        this.serializer = redissonClient.getConfig().getCodec().getValueEncoder();
        this.deserializer = redissonClient.getConfig().getCodec().getValueDecoder();
    }

    @Override
    public void set(byte[] key, byte[] value) {
        cluster.set(key, value);
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        return cluster.setnx(key, value);
    }

    @Override
    public void setex(byte[] key, int seconds, byte[] value) {
        cluster.setex(key, seconds, value);
    }

    @Override
    public byte[] get(byte[] key) {
        return cluster.get(key);
    }

    @Override
    public void mset(String... keyValue) {
        if (keyValue == null || keyValue.length == 0) {
            return;
        }
        Map<String, Object> keyValueMap = Maps.newConcurrentMap();
        for (int i = 0; i < keyValue.length; i += 2) {
            keyValueMap.put(keyValue[i], keyValue[i + 1]);
        }
        mset(keyValueMap);
    }

    @Override
    public void msetex(int seconds, String... keyValue) {
        if (keyValue == null || keyValue.length == 0) {
            return;
        }
        Map<String, Object> keyValueMap = Maps.newConcurrentMap();
        for (int i = 0; i < keyValue.length; i += 2) {
            keyValueMap.put(keyValue[i], keyValue[i + 1]);
        }
        msetex(seconds, keyValueMap);
    }

    @Override
    public void mset(Map<String, Object> keyValues) {
        if (keyValues == null || keyValues.size() == 0) {
            return;
        }
        if (BatchModel.ITERATION.equals(batchModel)) {
            for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
                try {
                     cluster.set(serializer.encode(entry.getKey()).array(),serializer.encode(entry.getValue()).array());
                } catch (Exception e) {
                    throw new RedisClientException("cluster mset by ITERATION error: ", e);
                }
            }
        } else {
            pipelineMset(keyValues);
        }
    }

    private void pipelineMset(Map<String, Object> keyValues) {
        RedissonClient redissonClient = getRedissonClient();
        //pipeline operation
        RBatch batch = redissonClient.createBatch();
        for (Map.Entry<String, Object> kv : keyValues.entrySet()) {
            batch.getBucket(kv.getKey()).setAsync(kv.getValue());
        }
        try {
            batch.execute();
        } catch (RedisException e) {
            throw new RedisClientException("cluster mset error", e);
        }
    }

    @Override
    public void msetex(int seconds, Map<String, Object> keyValues) {
        if (keyValues == null || keyValues.size() == 0) {
            return;
        }
        if (BatchModel.ITERATION.equals(batchModel)) {
            for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
                try {            	 
                    cluster.setex( serializer.encode(entry.getKey()).array(), seconds, serializer.encode(entry.getValue()).array());
                } catch (Exception e) {
                    throw new RedisClientException("cluster mset by ITERATION error: ", e);
                }
            }
        } else {
            pipelineMsetex(seconds, keyValues);
        }
    }

    private void pipelineMsetex(int seconds, Map<String, Object> keyValues) {
        RedissonClient redissonClient = getRedissonClient();
        //pipeline operation
        RBatch batch = redissonClient.createBatch();
        for (Map.Entry<String, Object> kv : keyValues.entrySet()) {
            batch.getBucket(kv.getKey()).setAsync(kv.getValue(), seconds, TimeUnit.SECONDS);
        }
        try {
            batch.execute();
        } catch (RedisException e) {
            throw new RedisClientException("cluster mset error", e);
        }
    }

    /**
     * 默认序列化为string
     *
     * @param keys
     * @return
     */
    @Override
    public List<String> mget(String... keys) {
        if (keys == null || keys.length == 0) {
            return null;
        }
        if (BatchModel.ITERATION.equals(batchModel)) {
            List<String> list = Lists.newArrayList();
            for (String key : keys) {
                try {
                    String value = cluster.get(key);
                    list.add(value);
                } catch (Exception e) {
                    throw new RedisClientException("cluster mget by ITERATION error: ", e);
                }
            }
            return list;
        } else {
            return pipelineMget(keys);
        }
    }

    private List<String> pipelineMget(String[] keys) {
        RedissonClient redissonClient = getRedissonClient();
        List<String> result = Lists.newArrayList();
        //pipeline operation
        RBatch batch = redissonClient.createBatch();
        for (String key : keys) {
            batch.getBucket(key).getAsync();
        }
        try {
            List<?> execute = batch.execute();
            List<String> transform = Lists.transform(execute, new Function<Object, String>() {
                @Override
                public String apply(Object input) {
                    return input != null ? input.toString() : null;
                }
            });
            result.addAll(transform);
        } catch (RedisException e) {
            throw new RedisClientException("cluster mget error", e);
        }
        return result;
    }

    @Override
    public List<Object> mget(Object... keys) {
        if (keys == null || keys.length == 0) {
            return null;
        }
        if (BatchModel.ITERATION.equals(batchModel)) {
            List<Object> list = Lists.newArrayList();
            for (Object key : keys) {
                try {
                    byte[] bytes = cluster.get(serializer.encode(key.toString()).array());
                    Object decode = null;
                    if (bytes != null && bytes.length > 0) {
                        ByteBuf byteBuf = Unpooled.buffer();
                        byteBuf.writeBytes(bytes);
                        decode = deserializer.decode(byteBuf, null);
                    }
                    list.add(decode);
                } catch (Exception e) {
                    throw new RedisClientException("cluster mget by ITERATION error: ", e);
                }
            }
            return list;
        } else {
            return pipelineMget(keys);
        }
    }

    private List<Object> pipelineMget(Object[] keys) {
        RedissonClient redissonClient = getRedissonClient();
        List<Object> result = Lists.newArrayList();
        //pipeline operation
        RBatch batch = redissonClient.createBatch();
        for (Object key : keys) {
            batch.getBucket(key.toString()).getAsync();
        }
        try {
            List<?> execute = batch.execute();
            result.addAll(execute);
        } catch (RedisException e) {
            throw new RedisClientException("cluster mget error", e);
        }
        return result;
    }

    @Override
    public Long incr(byte[] key) {
        return cluster.incr(key);
    }

    @Override
    public Long incrBy(byte[] key, long increment) {
        return cluster.incrBy(key, increment);
    }

    @Override
    public Double incrByFloat(byte[] key, double increment) {
        return cluster.incrByFloat(key, increment);
    }

    @Override
    public Long decr(byte[] key) {
        return cluster.decr(key);
    }

    @Override
    public Long decrBy(byte[] key, long decrement) {
        return cluster.decrBy(key, decrement);
    }

    @Override
    public Long del(byte[] key) {
        return cluster.del(key);
    }

    @Override
    public void expire(byte[] key, int seconds) {
        cluster.expire(key, seconds);
    }

    @Override
    public boolean exists(byte[] key) {
        return cluster.exists(key);
    }

    @Override
    public Long ttl(byte[] key) {
        return cluster.ttl(key);
    }

    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        return cluster.setbit(key, offset, value);
    }

    @Override
    public Boolean getbit(byte[] key, long offset) {
        return cluster.getbit(key, offset);
    }

    @Override
    public Long bitcount(byte[] key) {
        return cluster.bitcount(key);
    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        return cluster.bitcount(key, start, end);
    }

    @Override
    public byte[] lindex(byte[] key, long index) {
        return cluster.lindex(key, index);
    }

    @Override
    public Long llen(byte[] key) {
        return cluster.llen(key);
    }

    @Override
    public Long lpush(byte[] key, byte[]... value) {
        return cluster.lpush(key, value);
    }

    @Override
    public Long rpush(byte[] key, byte[]... value) {
        return cluster.rpush(key, value);
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long stop) {
        return cluster.lrange(key, start, stop);
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        return cluster.lrem(key, count, value);
    }

    @Override
    public void lset(byte[] key, long index, byte[] value) {
        cluster.lset(key, index, value);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return cluster.hget(key, field);
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return cluster.hset(key, field, value);
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return cluster.hsetnx(key, field, value);
    }

    @Override
    public Collection<byte[]> hval(byte[] key) {
        return cluster.hvals(key);
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return cluster.hmset(key, hash);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return cluster.hgetAll(key);
    }

    @Override
    public Long hdel(byte[] key, byte[] field) {
        return cluster.hdel(key, field);
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        return cluster.hkeys(key);
    }

    @Override
    public Long hlen(byte[] key) {
        return cluster.hlen(key);
    }

    @Override
    public boolean hexists(byte[] key, byte[] field) {
        return cluster.hexists(key, field);
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long increment) {
        return cluster.hincrBy(key, field, increment);
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double increment) {
        return cluster.hincrByFloat(key, field, increment);
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return cluster.hmget(key, fields);
    }

    @Override
    public Long sadd(byte[] key, byte[] value) {
        return cluster.sadd(key, value);
    }

    @Override
    public Long scard(byte[] key) {
        return cluster.scard(key);
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        return cluster.smembers(key);
    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        return cluster.sismember(key, member);
    }

    @Override
    public byte[] spop(byte[] key) {
        return cluster.spop(key);
    }

    @Override
    public Set<byte[]> spop(byte[] key, int count) {
        return cluster.spop(key, count);
    }

    @Override
    public byte[] srandommember(byte[] key) {
        return cluster.srandmember(key);
    }

    @Override
    public List<byte[]> srandommember(byte[] key, int count) {
        return cluster.srandmember(key, count);
    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMap) {
        ZAddParams params = ZAddParams.zAddParams();
        return cluster.zadd(key, scoreMap, params);
    }

    @Override
    public Long srem(byte[] key, byte[]... member) {
        return cluster.srem(key, member);
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        return cluster.sunion(keys);
    }

    @Override
    public Long sunionstore(byte[] destination, byte[]... keys) {
        return cluster.sunionstore(destination, keys);
    }

    @Override
    public Long smove(byte[] sourceKey, byte[] destKey, byte[] member) {
        return cluster.smove(sourceKey, destKey, member);
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        return cluster.sdiff(keys);
    }

    @Override
    public Long sdiffstore(byte[] destination, byte[]... keys) {
        return cluster.sdiffstore(destination, keys);
    }

    @Override
    public Set<byte[]> sinter(byte[] keys) {
        return cluster.sinter(keys);
    }

    @Override
    public Long sinterstore(byte[] destination, byte[]... keys) {
        return cluster.sinterstore(destination, keys);
    }

    @Override
    public Long zcard(byte[] key) {
        return cluster.zcard(key);
    }

    @Override
    public long zcount(byte[] key, double min, double max) {
        return cluster.zcount(key, min, max);
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        return cluster.zincrby(key, score, member);
    }

    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        return cluster.zrange(key, start, end);
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        return cluster.zrevrange(key, start, end);
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        return cluster.zrangeWithScores(key, start, end);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        return cluster.zrevrangeWithScores(key, start, end);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return cluster.zrangeByScore(key, min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return cluster.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return cluster.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return cluster.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return cluster.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return cluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Double zscore(byte[] key, byte[] member) {
        return cluster.zscore(key, member);
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        return cluster.zrevrank(key, member);
    }

    @Override
    public Long zrem(String key, String... members) {
        return cluster.zrem(key, members);
    }

    public void close() {

    }


    /**
     * 针对keys按照槽分区
     *
     * @param redisson
     * @param keys
     * @return
     */
    private Map<MasterSlaveEntry, List<String>> partition(Redisson redisson, Iterable<String> keys) {
        ClusterConnectionManager connectionManager = (ClusterConnectionManager) redisson.getConnectionManager();
        Map<MasterSlaveEntry, List<String>> partitioned = new HashMap<>();
        for (String key : keys) {
            int slot = connectionManager.calcSlot(key);
            MasterSlaveEntry entry = connectionManager.getEntry(slot);
            if (!partitioned.containsKey(entry)) {
                partitioned.put(entry, new ArrayList<String>());
            }
            Collection<String> list = partitioned.get(entry);
            list.add(key);
        }
        return partitioned;
    }


}
