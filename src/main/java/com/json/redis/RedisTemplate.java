package com.json.redis;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.json.redis.conf.RedisConfiguration;
import com.json.redis.connection.AbstractConnection;
import com.json.redis.connection.Connection;
import com.json.redis.connection.ConnectionUtils;
import com.json.redis.connection.factory.ConnectionFactory;
import com.json.redis.connection.factory.ConnectionFactoryBuilder;
import com.json.redis.exception.RedisClientException;
import com.json.redis.serializer.Deserializer;
import com.json.redis.serializer.Serializer;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import redis.clients.jedis.Tuple;
import redis.clients.util.SafeEncoder;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by   on 12/21/16.
 */
public class RedisTemplate {

    private String namespace;

    private ConnectionFactory connectionFactory;

    private RedisConfiguration redisConfiguration;

    private Serializer serializer;

    private Deserializer deserializer;

    @PostConstruct
    public void init() {
        RedisConfiguration redisConfiguration = getRedisConfiguration();
        redisConfiguration.setSerializer(serializer);
        redisConfiguration.setDeserializer(deserializer);
        connectionFactory = new ConnectionFactoryBuilder().setConfiguration(redisConfiguration).build();
        if (connectionFactory == null) {
            throw new RedisClientException("init redis connection is null. please set cluster configuration or shard configuration");
        }
        //兼容之前已有不包含namespace的key,切当前版本客户端的时候,namespace可以设置为""
        if (namespace == null) {
            throw new RedisClientException("please set client namespace");
        }
    }

    @PreDestroy
    public void destroy() {
        connectionFactory.close();
    }

    /**
     * 存储Integer类型
     *
     * @param key   Redis Key
     * @param value Integer value
     */
    public void setInt(String key, Integer value) {
        setWithoutSerializer(key, value);
    }

    /**
     * 存储Integer类型并设置过期时间
     *
     * @param key     Redis Key
     * @param value   Integer value
     * @param seconds Expire time
     */
    public void setInt(String key, Integer value, int seconds) {
        setWithoutSerializer(key, value, seconds);
    }

    /**
     * 获取Integer类型
     *
     * @param key Redis Key
     * @return Integer value or null only if key not exists
     */
    public Integer getInt(String key) {
        String value = getWithoutDeserializer(key);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new RedisClientException("not a integer value", e);
        }
    }

    /**
     * 存储Long类型
     *
     * @param key
     * @param value
     */
    public void setLong(String key, Long value) {
        setWithoutSerializer(key, value);
    }

    public void setLong(String key, Long value, int seconds) {
        setWithoutSerializer(key, value, seconds);
    }

    public Long getLong(String key) {
        String value = getWithoutDeserializer(key);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new RedisClientException("not a long value", e);
        }
    }

    public void setFloat(String key, Float value) {
        setWithoutSerializer(key, value);
    }

    public void setFloat(String key, Float value, int seconds) {
        setWithoutSerializer(key, value, seconds);
    }

    public Float getFloat(String key) {
        String value = getWithoutDeserializer(key);
        if (value == null) {
            return null;
        }
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            throw new RedisClientException("not a float value", e);
        }
    }

    public void setDouble(String key, Double value) {
        setWithoutSerializer(key, value);
    }

    public void setDouble(String key, Double value, int seconds) {
        setWithoutSerializer(key, value, seconds);
    }

    public Double getDouble(String key) {
        String value = getWithoutDeserializer(key);
        if (value == null) {
            return null;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new RedisClientException("not a double value", e);
        }
    }

    public void setString(String key, String value) {
        setWithoutSerializer(key, value);
    }

    public void setString(String key, String value, int seconds) {
        setWithoutSerializer(key, value, seconds);
    }

    public String getString(String key) {
        return getWithoutDeserializer(key);
    }

    public void mset(List<MSetKeyValues> keyValues) {
        if (keyValues == null) {
            return;
        }
        if (keyValues.size() == 0) {
            return;
        }
        String[] array = new String[keyValues.size() * 2];
        int j = 0;
        for (int i = 0; i < keyValues.size(); i++) {
            MSetKeyValues keyValue = keyValues.get(i);
            if (keyValue == null) {
                continue;
            }
            array[j] = buildKeyString(keyValue.getKey());
            array[j + 1] = keyValue.getValue();
            j += 2;
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.mset(array);
        } catch (Exception e) {
            throw new RedisClientException("set key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }

    }

    public void msetex(List<MSetKeyValues> keyValues, int seconds) {
        if (keyValues == null) {
            return;
        }
        if (keyValues.size() == 0) {
            return;
        }
        String[] array = new String[keyValues.size() * 2];
        int j = 0;
        for (int i = 0; i < keyValues.size(); i++) {
            MSetKeyValues keyValue = keyValues.get(i);
            if (keyValue == null) {
                continue;
            }
            array[j] = buildKeyString(keyValue.getKey());
            array[j + 1] = keyValue.getValue();
            j += 2;
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.msetex(seconds, array);
        } catch (Exception e) {
            throw new RedisClientException("set key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }

    }

    public void mset(Map<String, Object> keyValues) {
        if (keyValues == null || keyValues.size() == 0) {
            return;
        }
        Map<String, Object> kvMap = Maps.newConcurrentMap();
        for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
            kvMap.put(buildKeyString(entry.getKey()), entry.getValue());
        }

        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.mset(kvMap);
        } catch (Exception e) {
            throw new RedisClientException("set key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public void msetex(Map<String, Object> keyValues, int seconds) {
        if (keyValues == null || keyValues.size() == 0) {
            return;
        }
        Map<String, Object> kvMap = Maps.newConcurrentMap();
        for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
            kvMap.put(buildKeyString(entry.getKey()), entry.getValue());
        }

        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.msetex(seconds, kvMap);
        } catch (Exception e) {
            throw new RedisClientException("set key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public List<String> mget(String... keys) {
        if (keys == null || keys.length <= 0) {
            return null;
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            List<String> actualKeys = new ArrayList<>();
            for (int i = 0; i < keys.length; i++) {
                actualKeys.add(buildKeyString(keys[i]));
            }
            return connection.mget(actualKeys.toArray(new String[keys.length]));
        } catch (Exception e) {
            throw new RedisClientException("set key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> List<T> mget(Class<T> clazz, String... keys) {
        return mget((Type) clazz, keys);
    }

    public <T> List<T> mget(Type type, String... keys) {

        if ((String.class.equals(type))) {
            return (List<T>) mget(keys);
        }

        if (keys == null || keys.length <= 0) {
            return null;
        }

        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            Object[] actualKeys = new Object[keys.length];
            for (int i = 0; i < keys.length; i++) {
                actualKeys[i] = buildKeyString(keys[i]);
            }
            List<Object> values = connection.mget(actualKeys);

            if (values == null || values.size() == 0) {
                return null;
            }
            List<T> actual = new ArrayList<>();
            for (Object value : values) {
                if (value == null) {
                    actual.add(null);
                } else {
                    T result = null;
                    //todo:refactor here later
                    if (value instanceof JSONObject) {
                        JSONObject json = (JSONObject) value;
                        Class clazz = (Class) type;
                        result = (T) json.toJavaObject(clazz);
                    } else {
                        result = (T) value;
                    }
                    actual.add(result);
                }
            }
            return actual;

        } catch (Exception e) {
            throw new RedisClientException("mget key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public void setWithoutSerializer(String key, Object value) {
        setWithoutSerializer(key, value, -1);
    }

    public void setWithoutSerializer(String key, Object value, int seconds) {
        Assert.notNull(key);
        if (value == null) {
            return;
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            if (seconds > 0) {
                connection.setex(buildKey(key), seconds, value.toString().getBytes());
            } else {
                connection.set(buildKey(key), serializer.serializer(value));
            }
        } catch (Exception e) {
            throw new RedisClientException("set key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public void set(String key, Object value) {
        set(key, value, -1);
    }

    public void set(String key, Object value, int seconds) {
        Assert.notNull(key);
        if (value == null) {
            return;
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            if (seconds > 0) {
                connection.setex(buildKey(key), seconds, serializer.serializer(value));
            } else {
                connection.set(buildKey(key), serializer.serializer(value));
            }
        } catch (Exception e) {
            throw new RedisClientException("set key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public String getWithoutDeserializer(String key) {
        Assert.notNull(key);
        Connection connection = null;
        byte[] array;
        try {
            connection = connectionFactory.createConnection();
            array = connection.get(buildKey(key));
            if (array == null) {
                return null;
            }
            return SafeEncoder.encode(array);
        } catch (Exception e) {
            throw new RedisClientException("get key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> T get(String key, Class<T> clazz) {
        return get(key, (Type) clazz);
    }

    public <T> T get(String key, Type type) {
        Assert.notNull(key);
        Connection connection = null;
        byte[] array;
        try {
            connection = connectionFactory.createConnection();
            array = connection.get(buildKey(key));
            if (array == null) {
                return null;
            }
            return deserializer.deserializer(array, type);
        } catch (Exception e) {
            throw new RedisClientException("get key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long incr(String key) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.incr(buildKey(key));
        } catch (Exception e) {
            throw new RedisClientException("incr by error:", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long incrBy(String key, long increment) {
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.incrBy(buildKey(key), increment);
        } catch (Exception e) {
            throw new RedisClientException("incr error:", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Double incrByFloat(String key, double increment) {
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.incrByFloat(buildKey(key), increment);
        } catch (Exception e) {
            throw new RedisClientException("incr by error:", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long decr(String key) {
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.decr(buildKey(key));
        } catch (Exception e) {
            throw new RedisClientException("decr error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long decrBy(String key, long decrement) {
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.decrBy(buildKey(key), decrement);
        } catch (Exception e) {
            throw new RedisClientException("decr by error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long llen(String key) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.llen(buildKey(key));
        } catch (Exception e) {
            throw new RedisClientException("", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> void lpush(String key, T value) {
        lpush(key, value, -1);
    }

    public <T> void lpush(String key, T value, int seconds) {
        Assert.notNull(key);
        if (value == null) {
            return;
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            byte[] keyBytes = buildKey(key);
            connection.lpush(keyBytes, serializer.serializer(value));
            if (seconds > 0) {
                connection.expire(keyBytes, seconds);
            }
        } catch (Exception e) {
            throw new RedisClientException("add value to list error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> void createList(String key, List<T> value) {
        createList(key, value, -1);
    }

    public <T> void createList(String key, List<T> value, int second) {
        delete(key);
        rpush(key, value, second);
    }

    public <T> void rpush(String key, List<T> value) {
        rpush(key, value, -1);
    }

    public <T> void rpush(String key, List<T> value, int seconds) {
        Assert.notNull(key);
        if (value == null || value.size() == 0) {
            return;
        }
        Connection connection = null;
        byte[] keyBytes = buildKey(key);
        try {
            connection = connectionFactory.createConnection();
            byte[][] data = new byte[value.size()][];
            for (int i = 0; i < value.size(); i++) {
                data[i] = serializer.serializer(value.get(i));
            }
            connection.rpush(keyBytes, data);
            if (seconds > 0) {
                connection.expire(keyBytes, seconds);
            }
        } catch (Exception e) {
            throw new RedisClientException("add value to list error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> List<T> lrange(String key, int start, int end, Class<T> clazz) {
        return lrange(key, start, end, (Type) clazz);
    }

    public <T> List<T> lrange(String key, int start, int end, Type type) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            List<byte[]> list = connection.lrange(buildKey(key), start, end);
            if (list == null) {
                return null;
            }
            List<T> data = new ArrayList<>(list.size());
            for (byte[] buff : list) {
                data.add((T) deserializer.deserializer(buff, type));
            }
            return data;
        } catch (Exception e) {
            throw new RedisClientException("lrange error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> void lset(String key, int index, T value) {
        Assert.notNull(key);
        if (value != null) {
            Connection connection = null;
            try {
                connection = connectionFactory.createConnection();
                connection.lset(buildKey(key), index, serializer.serializer(value));
            } catch (Exception e) {
                throw new RedisClientException("lset error " + index, e);
            } finally {
                ConnectionUtils.releaseConnection(connection);
            }
        }
    }

    public <T> T lindex(String key, long index, Class<T> clazz) {
        return lindex(key, index, (Type) clazz);
    }

    public <T> T lindex(String key, long index, Type type) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            byte[] data = connection.lindex(buildKey(key), index);
            if (data == null) {
                return null;
            }
            return (T) deserializer.deserializer(data, type);
        } catch (Exception e) {
            throw new RedisClientException("lindex error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> Long lrem(String key, int count, T value) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.lrem(buildKey(key), count, serializer.serializer(value));
        } catch (Exception e) {
            throw new RedisClientException("lrem error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }

    }

    public <K, V> void hmset(String key, Map<K, V> map) {
        hmset(key, map, -1);
    }

    public <K, V> void hmset(String key, Map<K, V> map, int seconds) {
        Assert.notNull(key);
        if (map == null || map.size() == 0) {
            return;
        }
        Connection connection = null;
        byte[] keyBytes = buildKey(key);
        try {
            connection = connectionFactory.createConnection();
            Map<byte[], byte[]> hash = new HashMap<>();
            for (K mapKey : map.keySet()) {
                hash.put(serializer.serializer(mapKey), serializer.serializer(map.get(mapKey)));
            }
            connection.hmset(keyBytes, hash);
            if (seconds > 0) {
                connection.expire(keyBytes, seconds);
            }
        } catch (Exception e) {
            throw new RedisClientException("set map to redis error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <K, V> void hset(String key, K mapKey, V value) {
        hset(key, mapKey, value, -1);
    }

    public <K, V> void hset(String key, K mapKey, V value, int second) {
        Map<K, V> map = new HashMap<>();
        map.put(mapKey, value);
        hmset(key, map, second);
    }

    public <K, V> Map<K, V> hgetAll(String key, Class<K> keyClass, Class<V> valueClass) {
        return hgetAll(key, keyClass, (Type) valueClass);
    }

    public <K, V> Map<K, V> hgetAll(String key, Class<K> keyClass, Type type) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            Map<byte[], byte[]> redisMap = connection.hgetAll(buildKey(key));
            if (redisMap == null || redisMap.size() == 0) {
                return null;
            }
            Map<K, V> map = new HashMap<>();
            for (byte[] keyBytes : redisMap.keySet()) {
                byte[] value = redisMap.get(keyBytes);
                if (value == null) {
                    continue;
                }
                map.put((K) deserializer.deserializer(keyBytes, keyClass), (V) deserializer.deserializer(value, type));
            }
            return map;
        } catch (Exception e) {
            throw new RedisClientException("hgetAll error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <K, V> V hget(String key, K mapKey, Class<V> clazz) {
        return hget(key, mapKey, (Type) clazz);
    }

    public <K, V> V hget(String key, K mapKey, Type type) {
        Assert.notNull(key);
        Assert.notNull(mapKey);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            byte[] data = connection.hget(buildKey(key), serializer.serializer(mapKey));
            return deserializer.deserializer(data, type);
        } catch (Exception e) {
            throw new RedisClientException("hget error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <K, V> Long hsetnx(String key, K mapKey, V value) {
        Assert.notNull(key);
        Assert.notNull(mapKey);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.hsetnx(buildKey(key), serializer.serializer(mapKey), serializer.serializer(value));
        } catch (Exception e) {
            throw new RedisClientException("", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> List<T> hvals(String key, Class<T> clazz) {
        return hvals(key, (Type) clazz);
    }

    public <T> List<T> hvals(String key, Type type) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            Collection<byte[]> collection = connection.hval(buildKey(key));
            if (collection == null || collection.size() == 0) {
                return null;
            }
            List<T> list = new ArrayList<>(collection.size());
            for (byte[] data : collection) {
                T instance = deserializer.deserializer(data, type);
                list.add(instance);
            }
            return list;
        } catch (Exception e) {
            throw new RedisClientException("", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <K> void hdel(String key, K mapKey) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.hdel(buildKey(key), serializer.serializer(mapKey));
        } catch (Exception e) {
            throw new RedisClientException("remove map filed error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> boolean hexists(String key, T field) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.hexists(buildKey(key), serializer.serializer(field));
        } catch (Exception e) {
            throw new RedisClientException("hexists error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> Long hincrBy(String key, T field, long increment) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.hincrBy(buildKey(key), serializer.serializer(field), increment);
        } catch (Exception e) {
            throw new RedisClientException("hincrBy error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> Double hincrByFloat(String key, T field, double increment) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.hincrByFloat(buildKey(key), serializer.serializer(field), increment);
        } catch (Exception e) {
            throw new RedisClientException("hincrByFloat error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> List<T> hkeys(String key, Class<T> clazz) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            Set<byte[]> keys = connection.hkeys(buildKey(key));
            if (keys == null || keys.size() == 0) {
                return null;
            }
            List<T> keyList = new ArrayList<>(keys.size());
            for (byte[] mapKey : keys) {
                keyList.add((T) deserializer.deserializer(mapKey, clazz));
            }
            return keyList;
        } catch (Exception e) {
            throw new RedisClientException("get map keys error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public long hlen(String key) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.hlen(buildKey(key));
        } catch (Exception e) {
            throw new RedisClientException("get map size error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <K, V> List<V> hmget(String key, K[] fields, Class<V> clazz) {
        return hmget(key, fields, (Type) clazz);
    }

    public <K, V> List<V> hmget(String key, K[] fields, Type type) {
        Assert.notNull(key);

        if (fields == null || fields.length == 0) {
            return null;
        }

        byte[][] array = new byte[fields.length][];
        for (int i = 0; i < fields.length; i++) {
            array[i] = serializer.serializer(fields[i]);
        }

        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            List<byte[]> list = connection.hmget(buildKey(key), array);
            if (list == null || list.size() == 0) {
                return null;
            }
            List<V> list1 = new ArrayList<>(list.size());
            for (byte[] data : list) {
                V instance = deserializer.deserializer(data, type);
                list1.add(instance);
            }
            return list1;
        } catch (Exception e) {
            throw new RedisClientException("hmget error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> Long sadd(String key, T instance) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.sadd(buildKey(key), serializer.serializer(instance));
        } catch (Exception e) {
            throw new RedisClientException("add instance to set error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long scard(String key) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.scard(buildKey(key));
        } catch (Exception e) {
            throw new RedisClientException("scard error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> Set<T> sdiff(String[] keys, Class<T> clazz) {
        return sdiff(keys, (Type) clazz);
    }

    public <T> Set<T> sdiff(String[] keys, Type type) {
        Assert.notNull(keys);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            byte[][] datas = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) {
                datas[i] = serializer.serializer(keys[i]);
            }
            Set<byte[]> diffs = connection.sdiff(datas);
            if (diffs == null || diffs.size() == 0) {
                return null;
            }
            Set<T> set = new HashSet<>(diffs.size());
            for (byte[] data : diffs) {
                T instance = deserializer.deserializer(data, type);
                set.add(instance);
            }
            return set;
        } catch (Exception e) {
            throw new RedisClientException("sdiff error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long sdiffstore(String destinationKey, String[] keys) {
        Assert.notNull(keys);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            byte[][] datas = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) {
                datas[i] = serializer.serializer(keys[i]);
            }
            return connection.sdiffstore(buildKey(destinationKey), datas);
        } catch (Exception e) {
            throw new RedisClientException("sdiffstore error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> Long smove(String sourceKey, String destinationKey, T instance) {
        Assert.notNull(sourceKey);
        Assert.notNull(destinationKey);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.smove(buildKey(sourceKey), buildKey(destinationKey), serializer.serializer(instance));
        } catch (Exception e) {
            throw new RedisClientException("smove error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }

    }

    public Long delete(String key) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.del(buildKey(key));
        } catch (Exception e) {
            throw new RedisClientException("incr by error:", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long ttl(String key) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.ttl(buildKey(key));
        } catch (Exception e) {
            throw new RedisClientException("ttl by error:", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public boolean exists(String key) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.exists(buildKey(key));
        } catch (Exception e) {
            throw new RedisClientException("key exists error:", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public void expire(String key, int seconds) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            connection.expire(buildKey(key), seconds);
        } catch (Exception e) {
            throw new RedisClientException("expire key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> T doInLock(String lockName, int maxWait, int maxLockTimeAfterSuccess, RedisLockProcessor<T> proccessor) {
        Connection connection = null;
        boolean locked = false;
        try {
            connection = connectionFactory.createConnection();
            locked = connection.tryLock((namespace + ".lock." + lockName), maxWait, maxLockTimeAfterSuccess, TimeUnit.SECONDS);
            if (locked) {
                if (proccessor != null) {
                    return proccessor.doInLock(connection);
                }
            } else {
                throw new RedisClientException("cannot get lock from this thread");
            }
            return null;
        } catch (Exception e) {
            throw new RedisClientException(e.getMessage(), e);
        } finally {
            if (connection != null && locked) {
                connection.unlock();
            }
            ConnectionUtils.releaseConnection(connection);
        }
    }


    public boolean tryLock(String lockName, int maxWait, int maxLockTimeAfterSuccess) {
        return tryLock(lockName, maxWait, maxLockTimeAfterSuccess, TimeUnit.SECONDS);
    }

    public boolean tryLock(String lockName, int maxWait, int maxLockTimeAfterSuccess, TimeUnit unit) {
        Connection connection = null;
        boolean locked = false;
        try {
            connection = connectionFactory.createConnection();
            locked = connection.tryLock((namespace + ".lock." + lockName), maxWait, maxLockTimeAfterSuccess, unit);
            if (locked) {
                return locked;
            } else {
                throw new RedisClientException("cannot get lock from this thread");
            }
        } catch (Exception e) {
            throw new RedisClientException(e.getMessage(), e);
        } finally {
            if (connection != null) {
                ConnectionUtils.releaseConnection(connection);
            }
        }
    }

    public void unlock(String lockName) {
        Connection connection = null;
        RLock lock = null;
        try {
            connection = connectionFactory.createConnection();
            AbstractConnection abstractConnection = (AbstractConnection) connection;
            RedissonClient redissonClient = abstractConnection.getRedissonClient();
            lock = redissonClient.getLock(namespace + ".lock." + lockName);
            if (lock != null && lock.isLocked()) {
                lock.unlock();
            } else {
                throw new RedisClientException("lock is null or lock is expire!");
            }
        } catch (Exception e) {
            throw new RedisClientException(e.getMessage(), e);
        } finally {
            if (lock != null && lock.isLocked()) {
                forceUnlock(lockName);
            }
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public void forceUnlock(String lockName) {
        Connection connection = null;
        RLock lock = null;
        try {
            connection = connectionFactory.createConnection();
            AbstractConnection abstractConnection = (AbstractConnection) connection;
            RedissonClient redissonClient = abstractConnection.getRedissonClient();
            lock = redissonClient.getLock(namespace + ".lock." + lockName);
            if (lock != null && lock.isLocked()) {
                lock.forceUnlock();
            } else {
                throw new RedisClientException("lock is null or lock is expire!");
            }
        } catch (Exception e) {
            throw new RedisClientException(e.getMessage(), e);
        } finally {
            if (lock != null && lock.isLocked()) {
                lock.forceUnlock();
            }
            ConnectionUtils.releaseConnection(connection);
        }
    }


    public Long zadd(String key, String member, Double score) {
        if (StringUtils.isBlank(member) || score == null) {
            throw new RedisClientException("zadd parameter error");
        }
        Map<String, Double> map = new HashMap<>();
        map.put(member, score);
        return zadd(key, map);
    }

    public Long zadd(String key, Map<String, Double> scoreMap) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zadd key cannot be blank");
        }
        if (scoreMap == null || scoreMap.size() == 0) {
            throw new RedisClientException("zadd  map parameter error");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            Map<byte[], Double> map = new HashMap<>();
            for (String mapKey : scoreMap.keySet()) {
                if (mapKey == null) {
                    continue;
                }
                Double score = scoreMap.get(mapKey);
                if (score == null) {
                    continue;
                }
                map.put(mapKey.getBytes(), score);
            }
            return connection.zadd(buildKey(key), map);
        } catch (Exception e) {
            throw new RedisClientException("zadd error:", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Double zincrby(String key, double score, String member) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zincrby key cannot be blank");
        }
        if (StringUtils.isBlank(member)) {
            throw new RedisClientException("zincrby member cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zincrby(buildKey(key), score, member.getBytes());
        } catch (Exception e) {
            throw new RedisClientException("zincrby error:", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> Set<T> smembers(String key, Class<T> clazz) {
        return smembers(key, (Type) clazz);
    }

    public <T> Set<T> smembers(String key, Type type) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("smembers key cannot be blank");
        }
        Connection connection = null;

        try {
            connection = connectionFactory.createConnection();
            Set<byte[]> set = connection.smembers(buildKey(key));
            if (set == null || set.size() == 0) {
                return new HashSet<>();
            }
            Set<T> actualSet = new HashSet<>();
            for (byte[] data : set) {
                if (data == null) {
                    continue;
                }
                actualSet.add((T) deserializer.deserializer(data, type));
            }
            return actualSet;
        } catch (Exception e) {
            throw new RedisClientException("smembers key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public <T> T srandmember (String key, Class<T> clazz) {
        return srandmember(key, (Type) clazz);
    }
    
    public <T> Set<T> srandmember(String key, Class<T> clazz,int count) {
        return srandmember(key, (Type) clazz,count);
    }
    
    public <T> T srandmember(String key, Type type) {
        Assert.notNull(key);
        Connection connection = null;
        byte[] array;
        try {
            connection = connectionFactory.createConnection();
            array = connection.srandommember(buildKey(key));
            if (array == null) {
                return null;
            }
            return deserializer.deserializer(array, type);
        } catch (Exception e) {
            throw new RedisClientException("get key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }
    
    public <T> Set<T> srandmember(String key, Type type,int count) {
        Assert.notNull(key);
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            List<byte[]> set = connection.srandommember(buildKey(key),count);
            if (set == null || set.size() == 0) {
                return new HashSet<>();
            }
            Set<T> actualSet = new HashSet<>();
            for (byte[] data : set) {
                if (data == null) {
                    continue;
                }
                actualSet.add((T) deserializer.deserializer(data, type));
            }
            return actualSet;
        } catch (Exception e) {
            throw new RedisClientException("get key error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }
    
    
    public <T> Boolean sismember(String key, T instance) {
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.sismember(buildKey(key), serializer.serializer(instance));
        } catch (Exception e) {
            throw new RedisClientException("sismeber error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }

    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zincrby key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zrevrangeByScoreWithScores(buildKey(key), max, min, offset, count);
        } catch (Exception e) {
            throw new RedisClientException("zincrby error:", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public RedisConfiguration getRedisConfiguration() {
        return redisConfiguration;
    }

    public void setRedisConfiguration(RedisConfiguration redisConfiguration) {
        this.redisConfiguration = redisConfiguration;
    }

    //support get original connection for special use
    //pair use 4 releaseConnection
    public Connection getConnection() {
        return connectionFactory.createConnection();
    }

    public void releaseConnection(Connection connection) {
        ConnectionUtils.releaseConnection(connection);
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public void setDeserializer(Deserializer deserializer) {
        this.deserializer = deserializer;
    }

    public String buildKeyString(String key) {
        if (key == null) {
            throw new RedisClientException("key cannot be null");
        }
        String namespace = getNamespace();
        //兼容之前已有不包含namespace的key
        if (namespace == null || namespace.length() == 0) {
            return key;
        } else {
            return namespace + ":" + key;
        }
    }

    public byte[] buildKey(String key) {
        if (key == null) {
            throw new RedisClientException("key cannot be null");
        }
        return getBytes(buildKeyString(key));
    }

    public byte[] getBytes(String str) {
        if (str == null) {
            return null;
        }
        try {
            return str.getBytes(Constants.UTF8);
        } catch (Exception e) {
            return str.getBytes();
        }
    }


    public Double zscore(String key, String member) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zscore key cannot be blank");
        }
        if (StringUtils.isBlank(member)) {
            throw new RedisClientException("zscore member cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zscore(buildKey(key), getBytes(member));
        } catch (Exception e) {
            throw new RedisClientException("zscore error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long zrevrank(String key, String member) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zrevrank key cannot be blank");
        }
        if (StringUtils.isBlank(member)) {
            throw new RedisClientException("zrevrank member cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zrevrank(buildKey(key), getBytes(member));
        } catch (Exception e) {
            throw new RedisClientException("zrevrank error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    //位图操作
    public Boolean setbit(String key, long offset, boolean value) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("setbit key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.setbit(buildKey(key), offset, value);
        } catch (Exception e) {
            throw new RedisClientException("setbit error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Boolean getbit(String key, long offset) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("getbit key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.getbit(buildKey(key), offset);
        } catch (Exception e) {
            throw new RedisClientException("getbit error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long bitcount(final String key) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("bitcount key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.bitcount(buildKey(key));
        } catch (Exception e) {
            throw new RedisClientException("bitcount error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long bitcount(final String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("bitcount key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.bitcount(buildKey(key), start, end);
        } catch (Exception e) {
            throw new RedisClientException("bitcount error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long zrem(final String key, final String... members) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zrem key cannot be blank");
        }
        if (members == null || members.length == 0) {
            throw new RedisClientException("zrem member cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zrem(buildKeyString(key), members);
        } catch (Exception e) {
            throw new RedisClientException("zrem error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long zcount(final String key, double min, double max) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zcount key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zcount(buildKey(key), min, max);
        } catch (Exception e) {
            throw new RedisClientException("zcount error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key, double min, double max) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zrangeByScoreWithScores key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zrangeByScoreWithScores(buildKey(key), min, max);
        } catch (Exception e) {
            throw new RedisClientException("zrangeByScoreWithScores error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }


    public Long srem(String key, String... member) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("srem key cannot be blank");
        }
        if (member == null || member.length == 0) {
            throw new RedisClientException("srem member cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            byte[][] members = new byte[member.length][];
            for (int i = 0; i < member.length; i++) {
                members[i] = serializer.serializer(member[i]);
            }
            return connection.srem(buildKey(key), members);
        } catch (Exception e) {
            throw new RedisClientException("srem error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }


    public Set<byte[]> zrange(String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zrange key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zrange(buildKey(key), start, end);
        } catch (Exception e) {
            throw new RedisClientException("zrange error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zrangeWithScores key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zrangeWithScores(buildKey(key), start, end);
        } catch (Exception e) {
            throw new RedisClientException("zrangeWithScores error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Set<byte[]> zrevrange(String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zrevrange key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zrevrange(buildKey(key), start, end);
        } catch (Exception e) {
            throw new RedisClientException("zrevrange error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("zrevrangeWithScores key cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.zrevrangeWithScores(buildKey(key), start, end);
        } catch (Exception e) {
            throw new RedisClientException("zrevrangeWithScores error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    public Long setnx(String key, Object value) {
        if (StringUtils.isBlank(key)) {
            throw new RedisClientException("setnx key cannot be blank");
        }
        if (value == null) {
            throw new RedisClientException("setnx value cannot be blank");
        }
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            return connection.setnx(buildKey(key), serializer.serializer(value));
        } catch (Exception e) {
            throw new RedisClientException("setnx error", e);
        } finally {
            ConnectionUtils.releaseConnection(connection);
        }
    }

    // ***************************************** 兼容一些老接口 不要使用
    public boolean lock(final String keyStr, final int expireSeconds) {

		return tryLock(keyStr, expireSeconds, expireSeconds, TimeUnit.SECONDS);
	}

	public boolean lock(final String keyStr, final String value, final int expireSeconds) {

		return tryLock(keyStr, expireSeconds, expireSeconds, TimeUnit.SECONDS);
	}

	public boolean unLock(String key) {
		try {
			unlock(key);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

}
