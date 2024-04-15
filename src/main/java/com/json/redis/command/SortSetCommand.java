package com.json.redis.command;

import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;

/**
 * Created by   on 3/27/17.
 */
public interface SortSetCommand {

    Long zadd(byte[] key, Map<byte[], Double> scoreMembers);

    Long zcard(byte[] key);

    long zcount(byte[] key, double min, double max);

    Double zincrby(byte[] key, double score, byte[] member);

    Set<byte[]> zrange(byte[] key, long start, long end);

    Set<byte[]> zrevrange(byte[] key, long start, long end);

    Set<Tuple> zrangeWithScores(byte[] key, long start, long end);

    Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end);

    Set<byte[]> zrangeByScore(byte[] key, double min, double max);

    Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count);

    Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max);

    Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count);

    Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min);

    Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count);

    Double zscore(byte[] key, byte[] member);

    Long zrevrank(byte[] key, byte[] member);

    Long zrem(final String key, final String... members);

}
