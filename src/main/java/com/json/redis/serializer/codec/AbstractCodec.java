package com.json.redis.serializer.codec;

import java.lang.reflect.Type;

import com.json.redis.exception.RedisClientException;
import com.json.redis.serializer.Deserializer;
import com.json.redis.serializer.Serializer;

/**
 * Created by   on 3/24/17.
 */
public abstract class AbstractCodec implements Serializer, Deserializer {

    private static final int DEFAULT_SERIALIZER_SIZE = 10 * 1024 * 1024;
    private int serializerSize = DEFAULT_SERIALIZER_SIZE;

    @Override
    public <T> byte[] serializer(T instance) {
        if (instance == null){
            throw new RedisClientException("serializer instance cannot be null");
        }
        byte[] data = doSerializer(instance);
        if(data != null && data.length > serializerSize){
            throw new RedisClientException("data size exceed limit:" + data.length);
        }
        return data;
    }

    @Override
    public <T> T deserializer(byte[] data, Type type) {
        if (data == null){
            return null;
        }
        if (type == null){
            throw new RedisClientException("deserializer type cannot be null");
        }
        return doDeserializer(data, type);
    }

    protected abstract <T> T doDeserializer(byte[] data, Type type);

    protected abstract <T> byte[] doSerializer(T instance);

    public int getSerializerSize() {
        return serializerSize;
    }

    public void setSerializerSize(int serializerSize) {
        this.serializerSize = serializerSize;
    }
}
