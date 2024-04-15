package com.json.redis.command;

import java.util.List;

/**
 * Created by   on 12/22/16.
 */
public interface ListCommand {
    byte[] lindex(byte[] key, long index);

    Long llen(byte[] key);

    Long lpush(byte[] key, byte[]... value);

    Long rpush(byte[] key, byte[]... value);

    List<byte[]> lrange(byte[] key, long start, long stop);

    Long lrem(byte[] key, long count, byte[] value);

    void lset(byte[] key, long index, byte[] value);
}
