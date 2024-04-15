package com.json.redis.serializer.codec;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.lang.reflect.Type;

/**
 * Created by   on 12/21/16.
 */
public class JsonCodec extends AbstractCodec {

    public <T> byte[] doSerializer(T instance) {
        return JSON.toJSONBytes(instance, SerializerFeature.DisableCircularReferenceDetect);
    }

    public <T> T doDeserializer(byte[] data, Type type) {
        return JSON.parseObject(data, type);
    }



}
