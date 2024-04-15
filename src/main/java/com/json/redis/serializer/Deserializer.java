package com.json.redis.serializer;

import java.lang.reflect.Type;

/**
 * Created by   on 12/21/16.
 */
public interface Deserializer {

    <T> T deserializer(byte[] data, Type type);

}
