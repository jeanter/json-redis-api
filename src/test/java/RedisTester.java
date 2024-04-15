import com.alibaba.fastjson.JSON;
import com.json.redis.RedisTemplate;
import com.json.redis.conf.RedisClusterConfiguration;
import com.json.redis.serializer.codec.SerializationCodec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisTester {
    public static void main(String[] args) {
        RedisClusterConfiguration config = new RedisClusterConfiguration();
        config.setIpAddressList("192.168.24.31:7000,192.168.24.31:7001,192.168.24.31:7002,192.168.24.31:7003,192.168.24.31:7004,192.168.24.31:7005");
        config.setLockIpAddressList("192.168.24.30:7000,192.168.24.30:7001,192.168.24.30:7002,192.168.24.30:7003,192.168.24.30:7004,192.168.24.30:7005");
        config.setMaxIdle(10);
        config.setMaxTotal(10);
        config.setMinIdle(2);
        config.setTimeout(3000);
        SerializationCodec serial = new SerializationCodec();
        serial.setSerializerSize(10485760);
        RedisTemplate redisTemplate = new RedisTemplate();
        redisTemplate.setDeserializer(serial);
        redisTemplate.setSerializer(serial);
        redisTemplate.setRedisConfiguration(config);
        redisTemplate.setNamespace("");
        redisTemplate.init();
        RedisClusterConfiguration cache = new RedisClusterConfiguration();
        cache.setMaxIdle(10);
        cache.setIpAddressList("tester");

        RedisClusterConfiguration cache1 = new RedisClusterConfiguration();
        cache1.setMaxIdle(11);
        cache1.setIpAddressList("tester1");
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("key", cache);
        map.put("key1", cache1);
        redisTemplate.mset(map);
        List<RedisClusterConfiguration> cacheList = redisTemplate.mget(RedisClusterConfiguration.class, new String[]{"key1", "", "key"});
        System.out.println(JSON.toJSONString(cacheList));
    }
}
