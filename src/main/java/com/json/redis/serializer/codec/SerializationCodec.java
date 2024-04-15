package com.json.redis.serializer.codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.json.redis.exception.RedisClientException;

import java.io.*;
import java.lang.reflect.Type;

public class SerializationCodec extends AbstractCodec {

    private static Logger logger = LoggerFactory.getLogger(SerializationCodec.class);

    @Override
    protected <T> T doDeserializer(byte[] data, Type type) {
        if (null == data) {
            return null;
        }
        ByteArrayInputStream bais = null;
        ObjectInputStream ois = null;
        try {
            bais = new ByteArrayInputStream(data);
            ois = new ObjectInputStream(bais);
            Object o = ois.readObject();
            T instance = (T) o;
            return instance;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RedisClientException("doDeserializer error", e);
        } finally {
            try {
                if (null != ois) {
                    ois.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            try {
                if (null != bais) {
                    bais.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    protected <T> byte[] doSerializer(T instance) {
        if (null == instance) {
            return null;
        }
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(instance);
            byte[] bytes = baos.toByteArray();
            return bytes;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RedisClientException("doSerializer error", e);
        } finally {
            try {
                if (null != baos) {
                    baos.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            try {
                if (null != oos) {
                    oos.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
