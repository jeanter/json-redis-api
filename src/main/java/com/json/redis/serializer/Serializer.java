package com.json.redis.serializer;

/**
 * Created by   on 12/21/16.
 */
public interface Serializer {

    <T> byte[] serializer(T instance);

}
