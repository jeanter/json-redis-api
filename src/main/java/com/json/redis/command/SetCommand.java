package com.json.redis.command;

import java.util.List;
import java.util.Set;

/**
 * Created by   on 3/27/17.
 */
public interface SetCommand {

    Long sadd(byte[] key, byte[] value);

    Long scard(byte[] key);

    Set<byte[]> smembers(byte[] key);

    Boolean sismember(byte[] key, byte[] member);

    byte[] spop(byte[] key);

    Set<byte[]> spop(byte[] key, int count);

    byte[] srandommember(byte[] key);

    List<byte[]> srandommember(byte[] key, int count);

    Long srem(byte[] key, byte[]... member);

    Set<byte[]> sunion(byte[]... keys);

    Long sunionstore(byte[] destination, byte[]...keys);

    Long smove(byte[] sourceKey, byte[] destKey, byte[] member);

    Set<byte[]> sdiff(byte[]... keys);

    Long sdiffstore(byte[] destination, byte[]... keys);

    Set<byte[]> sinter(byte[] keys);

    Long sinterstore(byte[] destination, byte[]... keys);

}
