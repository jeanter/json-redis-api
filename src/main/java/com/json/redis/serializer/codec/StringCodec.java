package com.json.redis.serializer.codec;

import java.lang.reflect.Type;
import java.nio.charset.Charset;

import com.json.redis.Assert;
import com.json.redis.exception.RedisClientException;

/**
 * @author  
 */
public class StringCodec extends AbstractCodec {

    public <T> byte[] doSerializer(T instance) {
        if (!(instance instanceof String)) {
            throw new RedisClientException("Serializer Object is not String");
        }
        String string = (String) instance;
        return (string == null ? null : string.getBytes(charset));
    }

    public <T> T doDeserializer(byte[] data, Type type) {
        if (!(String.class.equals(type))) {
            throw new RedisClientException("Deserializer type here only support String");
        }
        if (data == null || data.length == 0) {
            return null;
        } else {
            return (T) new String(data, charset);
        }
    }


    private final Charset charset;

    public StringCodec() {
        this(Charset.forName("UTF8"));
    }

    public StringCodec(Charset charset) {
        Assert.notNull(charset);
        this.charset = charset;
    }
}
