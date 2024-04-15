package com.json.redis.serializer.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

import com.json.redis.serializer.Deserializer;
import com.json.redis.serializer.Serializer;

import java.io.IOException;

/**
 * 使用代理方式
 * 保证redisson统一序列化方式
 */
public class RedissonCodec implements Codec {

    private Serializer serializer;
    private final Encoder encoder = new Encoder() {
        @Override
        public ByteBuf encode(Object in) throws IOException {
        	byte[] payload = serializer.serializer(in) ;
        	//堆内内存
        	ByteBuf out =Unpooled.buffer(payload.length);
        	out.writeBytes(payload);
            return out;
        }
    };
    private Deserializer deserializer;
    private final Decoder<Object> decoder = new Decoder<Object>() {
        @Override
        public Object decode(ByteBuf buf, State state) throws IOException {
            byte[] buffer = new byte[buf.readableBytes()];
            buf.readBytes(buffer);
            return deserializer.deserializer(buffer, Object.class);
        }
    };

    public RedissonCodec(Serializer serializer, Deserializer deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public Decoder<Object> getMapValueDecoder() {
        return decoder;
    }

    @Override
    public Encoder getMapValueEncoder() {
        return encoder;
    }

    @Override
    public Decoder<Object> getMapKeyDecoder() {
        return decoder;
    }

    @Override
    public Encoder getMapKeyEncoder() {
        return encoder;
    }

    @Override
    public Decoder<Object> getValueDecoder() {
        return decoder;
    }

    @Override
    public Encoder getValueEncoder() {
        return encoder;
    }
}
