package com.json.redis.command;

import java.util.List;
import java.util.Map;

/**
 * Created by   on 12/21/16.
 */
public interface StringCommand {

    void set(byte[] key, byte[] value);

    Long setnx(final byte[] key, final byte[] value);

    void setex(byte[] key, int seconds, byte[] value);

    byte[] get(byte[] key);

    void mset(String... keyValue);

    void msetex(int seconds, String... keyValue);

    void mset(Map<String, Object> keyValues);

    void msetex(int seconds, Map<String, Object> keyValues);

    //redis原生接口
    List<String> mget(String... keys);

    //为了扩充不同序列化方式
    List<Object> mget(Object... keys);

    Long incr(byte[] key);

    Long incrBy(byte[] key, long increment);

    Double incrByFloat(byte[] key, double increment);

    Long decr(byte[] key);

    Long decrBy(byte[] key, long decrement);

    Long del(byte[] key);

    void expire(byte[] key, int seconds);

    boolean exists(byte[] key);

    Long ttl(byte[] key);

    Boolean setbit(byte[] key, long offset, boolean value);

    Boolean getbit(byte[] key, long offset);

    Long bitcount(final byte[] key);

    Long bitcount(final byte[] key, long start, long end);

}
