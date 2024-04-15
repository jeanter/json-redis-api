package com.json.redis.command;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by   on 12/22/16.
 */
public interface HashCommand {

    byte[] hget(byte[] key, byte[] field);

    Long hset(byte[] key, byte[] field, byte[] value);

    Long hsetnx(byte[] key, byte[] field, byte[] value);

    Map<byte[], byte[]> hgetAll(byte[] key);

    Long hdel(byte[] key, byte[] field);

    Set<byte[]> hkeys(byte[] key);

    Long hlen(byte[] key);

    boolean hexists(byte[] key, byte[] field);

    Long hincrBy(byte[] key, byte[] field, long increment);

    Double hincrByFloat(byte[] key, byte[] field, double increment);

    List<byte[]> hmget(byte[] key, byte[]... fields);

    String hmset(byte[] key, Map<byte[], byte[]> hash);

    Collection<byte[]> hval(byte[] key);


}
