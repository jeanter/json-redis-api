import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.json.redis.MSetKeyValues;
import com.json.redis.RedisLockProcessor;
import com.json.redis.RedisTemplate;
import com.json.redis.conf.RedisClusterConfiguration;
import com.json.redis.conf.RedisConfiguration;
import com.json.redis.conf.RedisSingleConfiguration;
import com.json.redis.connection.Connection;
import com.json.redis.connection.RedisServer;
import com.json.redis.serializer.codec.JsonCodec;
import com.json.redis.serializer.codec.SerializationCodec;
import com.json.redis.serializer.codec.StringCodec;

import groovy.lang.GroovyClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.clients.jedis.Tuple;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by GodzillaHua on 12/22/16.
 */
public class SingleRedisTest implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(SingleRedisTest.class);
    private RedisTemplate template;
    RedisConfiguration configuration;

    @BeforeClass
    public void init() {
//        configuration = new RedisSingleConfiguration();
//        configuration.setIpAddressList("10.88.9.117:6379");
//        configuration.setLockIpAddressList("10.88.9.117:6379");
        configuration = new RedisClusterConfiguration();
//        configuration.setIpAddressList("192.168.24.50:7001,192.168.24.50:7002,192.168.24.50:7003,192.168.24.50:7004,192.168.24.50:7005,192.168.24.50:7006");
        configuration.setLockIpAddressList("192.168.24.50:7003,192.168.24.50:7004,192.168.24.50:7005");
        configuration.setIpAddressList("192.168.24.50:7003,192.168.24.50:7004,192.168.24.50:7005");
//        configuration.setIpAddressList("10.88.9.119:6379,10.88.9.119:6380,10.88.9.119:6381,10.88.9.69:6379,10.88.9.69:6380,10.88.9.69:6381");
//        configuration.setLockIpAddressList("10.88.9.119:6379,10.88.9.119:6380,10.88.9.119:6381,10.88.9.69:6379,10.88.9.69:6380,10.88.9.69:6381");
        configuration.setTimeout(3000);
        configuration.setRetryInterval(1000);
//        configuration.setPassword("meitunmama");
        configuration.setBatchModel(1);
        JsonCodec jsonCodec = new JsonCodec();
        SerializationCodec jdkCodec = new SerializationCodec();
        StringCodec stringCodec = new StringCodec();
//        configuration.setSerializer(jdkCodec);
//        configuration.setDeserializer(jdkCodec);
        template = new RedisTemplate();
        template.setNamespace("");
        template.setRedisConfiguration(configuration);
//        template.setSerializer(jsonCodec);
//        template.setDeserializer(jsonCodec);
//        template.setSerializer(stringCodec);
//        template.setDeserializer(stringCodec);
        template.setSerializer(jdkCodec);
        template.setDeserializer(jdkCodec);
        template.init();
    }

    @Test
    public void testSetString() {
        template.setInt("test", 1);
    }

    @Test
    public void testSetObject() {
        TestPojo server = new TestPojo();
        server.setName("haha");
        template.set("server", server);
        TestPojo redisServer = template.get("server", TestPojo.class);
        System.out.println(redisServer);
    }

    @Test
    public void testJDKSerialization() {

        StringBuilder sb = new StringBuilder();
        sb.append("import java.io.Serializable;\n" +
                "\n" +
                "public class TestPojo implements Serializable {\n" +
                "    private String name;\n" +
                "\n" +
                "    public String getName() {\n" +
                "        return name;\n" +
                "    }\n" +
                "\n" +
                "    public void setName(String name) {\n" +
                "        this.name = name;\n" +
                "    }\n" +
                "}\n");
        // 先用groovyClassLoader替换当前的classLoader
//        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        GroovyClassLoader groovyCl = new GroovyClassLoader();
        Class groovyClass = groovyCl.parseClass(sb.toString());
        Thread.currentThread().setContextClassLoader(groovyCl);
        Object price = template.get("server", groovyClass);
        System.out.println(JSON.toJSONString(price));
    }

    @Test
    public void testJDKSerializationString() {
        SerializationCodec jdkCodec = new SerializationCodec();
        String result = new String(jdkCodec.serializer(configuration));
        System.out.printf("jdkCodec string " + result);
        RedisConfiguration deserializer = jdkCodec.deserializer(result.getBytes(), RedisSingleConfiguration.class);
        System.out.printf("deserialize obj" + deserializer.getIpAddressList());
    }

    @Test
    public void testSetListObject() {
        List<RedisServer> servers = new ArrayList<>();
        RedisServer server = new RedisServer();
        for (int i = 0; i < 10; i++) {
            servers.add(server);
        }
        template.set("servers", servers);
        List<RedisServer> redisServersList = template.get("servers", new TypeReference<List<RedisServer>>() {
        }.getType());
        System.out.println(redisServersList);
    }

    @Test
    public void testMSetListObject() {
        List<MSetKeyValues> servers = new ArrayList<>();
        String[] keys = new String[20];
        for (int i = 0; i < 20; i++) {
            MSetKeyValues keyValues = new MSetKeyValues();
            keyValues.setKey("mset" + i);
            keyValues.setValue("value:" + i);
            servers.add(keyValues);
            keys[i] = keyValues.getKey();
        }
//        template.mset(servers);
        List<String> redisServersList = template.mget(keys);
        System.out.println(redisServersList);
    }

    @Test
    public void testMgetsetSerialization() {
        Map<String, Object> servers = Maps.newConcurrentMap();
        String[] keys = new String[2];
        for (int i = 0; i < 2; i++) {
            TestPojo keyValues = new TestPojo();
            keyValues.setName("testpojo" + i);
            servers.put(keyValues.getName(), keyValues.getName());
            keys[i] = keyValues.getName();
        }
        template.mset(servers);
        List<String> redisServersList = template.mget(String.class, keys);
        System.out.println(redisServersList);
    }


    @Test
    public void testMSetExListObject() throws InterruptedException {
        List<MSetKeyValues> servers = new ArrayList<>();
        String[] keys = new String[2];
        for (int i = 0; i < 2; i++) {
            MSetKeyValues keyValues = new MSetKeyValues();
            keyValues.setKey("mset" + i);
            keyValues.setValue("value:" + i);
            servers.add(keyValues);
            keys[i] = keyValues.getKey();
        }
        template.mset(servers);
        Thread.sleep(50);
        keys[1] = "xxx";
        List<String> redisServersList = template.mget(keys);
        System.out.println(redisServersList);
    }

    @Test
    public void testMSetMapObject() {
        String[] keys = new String[20];
        Map<String, Object> kvs = Maps.newConcurrentMap();
        for (int i = 0; i < 20; i++) {
            RedisServer redisServer = new RedisServer();
            redisServer.setHost("192.168.0.1" + i);
            redisServer.setPort(Integer.valueOf("700" + i));
            kvs.put("mmset" + i, redisServer);
            keys[i] = "mmset" + i;
        }
        template.mset(kvs);
        List<String> redisServersList = template.mget(keys);
        System.out.println(redisServersList);
    }


    @Test
    public void testMSetBigObject() {
        //generate big string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 9 * 1024 * 1024; i++) {
            sb.append("1");
        }

        String[] keys = new String[20];
        Map<String, Object> kvs = Maps.newConcurrentMap();
        for (int i = 0; i < 3; i++) {
            kvs.put("mmset" + i, sb.toString());
            keys[i] = "mmset" + i;
        }
        template.msetex(kvs, 1);
//        List<String> redisServersList = template.mget(keys);
//        System.out.println(redisServersList);
    }


    @Test
    public void testSetBigObject() {
        //generate big string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 9 * 1024 * 1024; i++) {
            sb.append("1");
        }

        for (int i = 0; i < 3; i++) {
            template.set("mmset" + i, sb.toString());
        }

//        List<String> redisServersList = template.mget(keys);
//        System.out.println(redisServersList);
    }


    @Test
    public void testSetBigObjectByThread() {
        //generate big string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1 * 1024 * 1024; i++) {
            sb.append("1");
        }

        for (int i = 0; i < 20; i++) {
            SetBigObjTask task = new SetBigObjTask("mmset" + i, sb.toString());
            completionService.submit(task);
        }

        while (true) {
            if (count.getCount() == 0) {
                for (int i = 0; i < 20; i++) {
                    try {
                        completionService.take().get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("SingleRedisTest.testSetBigObjectByThread");
                break;
            }
        }
    }

    public class SetBigObjTask implements Callable<String> {
        private String key;
        private String value;

        public SetBigObjTask(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String call() throws Exception {
            template.set(key, value);
            count.countDown();
            return null;
        }
    }


    @Test
    public void testMSetEXMapObject() throws InterruptedException {
        String[] keys = new String[20];
        Map<String, Object> kvs = Maps.newConcurrentMap();
        for (int i = 0; i < 20; i++) {
            RedisServer redisServer = new RedisServer();
            redisServer.setHost("192.168.0.1" + i);
            redisServer.setPort(Integer.valueOf("700" + i));
            kvs.put("mmset" + i, redisServer);
            keys[i] = "mmset" + i;
        }
        template.msetex(kvs, 1);
        Thread.sleep(500);
        List<String> redisServersList = template.mget(keys);
        System.out.println(redisServersList);
    }

    @Test
    public void testSetList() {
//        for (int i = 0; i < 20000; i++) {
//            RedisServer redisServer = new RedisServer();
//            redisServer.setPort(i);
//            template.set("set" + i, redisServer);
//        }
        List<String> redisServersList = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            String redisServers = template.get("mmset" + i, String.class);
            redisServersList.add(redisServers);
        }
        System.out.println(redisServersList);
    }


    public class MsetTask implements Callable<String> {
        private String key;

        public MsetTask(String key) {
            this.key = key;
        }

        public String call() throws Exception {
            Map<String, Object> kvs = Maps.newConcurrentMap();
            for (int i = 0; i < 20; i++) {
                RedisServer redisServer = new RedisServer();
                redisServer.setHost(key + " : 192.168.0.1" + i);
                redisServer.setPort(Integer.valueOf("700" + i));
                kvs.put("mmset" + i, redisServer);
            }
            template.mset(kvs);
            count.countDown();
            return null;
        }
    }


    @Test
    public void testMThread() {

        for (int i = 0; i < 20; i++) {
            MsetTask task = new MsetTask("testMThreadBatch" + i);
            completionService.submit(task);
        }

        while (true) {
            if (count.getCount() == 0) {
                testSetList();
                break;
            }
        }
    }


    @Test
    public void testIncr() {
//        Long incr = template.incr("testincr4");

        for (int i = 0; i < 2000; i++) {
            IncrTask task = new IncrTask("testincr6");
            completionService.submit(task);
        }

        Long testincr = template.get("testincr6", Long.TYPE);
        System.out.println(testincr);
    }

    public class IncrTask implements Callable<String> {
        private String key;

        public IncrTask(String key) {
            this.key = key;
        }

        public String call() throws Exception {
//            Long testincr = template.get(key, Long.TYPE);
            Long incr = template.incr(key);
//            System.out.println("thread id is:" + Thread.currentThread().getName() + " incr id is :" + incr);
            if (incr <= 20) {
                count.countDown();
                System.out.println("thread id is:" + Thread.currentThread().getName() + " get coupon id:" + incr);

            }
            return null;
        }
    }

    private static int numThread = 10;
    private static CountDownLatch count = new CountDownLatch(20);
    private static ExecutorService executor = Executors.newFixedThreadPool(numThread);
    private static CompletionService<String> completionService = new ExecutorCompletionService<String>(executor);


    public class Task implements Callable<String> {
        private String key;

        public Task(String key) {
            this.key = key;
        }

        public String call() throws Exception {
            String redisServersList = template.get(key, String.class);
            count.countDown();
            return redisServersList;
        }
    }

    @Test
    public void testGetKeyByMThread() {
        for (int i = 0; i < 20; i++) {
            Task task = new Task("mset" + i);
            completionService.submit(task);
        }
        while (true) {
            if (count.getCount() == 0) {
                List<String> redisServersList = Lists.newArrayList();
                for (int i = 0; i < 20; i++) {
                    try {
                        redisServersList.add(completionService.take().get());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println(redisServersList);
                break;
            }
        }

//        for (int i = 0; i < 100; i++) {
//            try {
//                System.out.println(completionService.take().get());
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }
//        }


    }

    @Test
    public void testBatchSetList() {

    }

    @Test
    public void testBatchGetList() {

    }


    @Test
    public void testGetListLength() {
        System.out.println(template.llen("ab"));
    }

    @Test
    public void testAddList() {
        template.lpush("ab", new RedisServer());
    }

    @Test
    public void testGetAllList() {
        List<RedisServer> servers = this.template.lrange("ab", 0, -1, RedisServer.class);
        System.out.println(servers);
    }

    @Test
    public void testGetObjectAtList() {
        System.out.println(this.template.lindex("ab", 2, RedisServer.class));
    }

    @Test
    public void testGetSetMap() {
        Map<String, RedisServer> map = new HashMap<>();
        map.put("server1", new RedisServer());
        map.put("server2", new RedisServer());
        map.put("server3", new RedisServer());
        map.put("server4", new RedisServer());
        map.put("server11", new RedisServer());

        //template.setMap("serverMap", map);

        Map<String, RedisServer> redisMap = this.template.hgetAll("serverMap", String.class, RedisServer.class);
        //System.out.println(redisMap);

        Map<String, List<RedisServer>> map1 = new HashMap<>();
        List<RedisServer> list = new ArrayList<>();
        list.add(new RedisServer("192.168.11.1", 6379));
        list.add(new RedisServer("192.168.11.2", 6378));
        list.add(new RedisServer("192.168.11.3", 6333));
        list.add(new RedisServer("192.168.11.4", 6300));
        list.add(new RedisServer("192.168.11.5", 6371));
        map1.put("server100", list);
        map1.put("server200", list);
        map1.put("server301", list);
        map1.put("server409", list);
        map1.put("server511", list);
        template.hmset("serverMap1", map1);

        Map<String, List<RedisServer>> server1 = this.template.hgetAll("serverMap1", String.class, new TypeReference<List<RedisServer>>() {
        }.getType());
        for (String key : server1.keySet()) {
            List<RedisServer> servers = server1.get(key);
            System.out.println(servers);
        }
    }

    @Test
    public void testNumericMap() {
        Map<Integer, String> value = new HashMap<>();
        value.put(1, "1");
        value.put(2, "2");
        value.put(3, "3");
        value.put(4, "4");
        value.put(5, "5");
        value.put(6, "6");
        this.template.hset("testMap", value, 30);

        Map<Integer, String> redisMap = this.template.hgetAll("testMap", Integer.class, String.class);
        System.out.println(redisMap);

        String v = this.template.hget("testMap", 2, String.class);
        System.out.println(v);
    }

    @Test
    public void testLock() {
        this.template.doInLock("test", 3, 5, new RedisLockProcessor<String>() {
            @Override
            public String doInLock(Connection connection) {
                try {
                    System.out.println("start");
                    long time = System.currentTimeMillis();
                    Thread.sleep(10 * 1000);
                    System.out.println(System.currentTimeMillis() - time);
                } catch (Exception e) {
                    logger.error("sth is wrong in the lock process", e);
                }
                return "haha,job is done!";
            }
        });
    }

    @Test
    public void testLockByLocker() {
        try {
            boolean lock = template.tryLock("test", 3, 10, TimeUnit.SECONDS);
            if (lock) {
                System.out.println("start");
                long time = System.currentTimeMillis();
                Thread.sleep(10 * 1000);
                System.out.println(System.currentTimeMillis() - time);
            }
        } catch (Exception e) {
            logger.error("sth is wrong in the lock process", e);
        } finally {
            template.unlock("test");
//            if this locked by other thread ,please use below
//            template.forceUnlock("test");
        }
    }

    @Test
    public void testBitOps() {
        for (int i = 0; i < 10; i++) {
            this.template.setbit("testbit", i, i % 2 == 0);
        }

        for (int i = 0; i < 10; i++) {
            Boolean testbit = template.getbit("testbit", i);
            System.out.println("index i: " + i + "  ,value :" + testbit);
        }

        Long v = this.template.bitcount("testbit");
        System.out.println("true value length is : " + v);
    }

    @Test
    public void test() {
        try {
            template.set("member_order_notuse_seconds_u132765701633", "1");
            Long integer = template.get("member_order_notuse_seconds_u132765701633", Long.class);
            System.out.println(integer);
            //incrby 只能用原生序列化方式,即字符串
            template.incrBy("member_order_notuse_seconds_u132765701633", 1);
            System.out.println(template.get("member_order_notuse_seconds_u132765701633", String.class));
        } catch (Exception e) {
            throw e;
        }
    }


    @Test
    public void testSortSet() {
        try {

            Map<String, Double> map = new HashMap<>();
            map.put("server1", 1d);
            map.put("server2", 2d);
            map.put("server3", 3d);
            map.put("server4", 4d);
            map.put("server11", 11d);

            template.zadd("zadd_test", map);
            Set<byte[]> zadd_test1 = template.zrange("zadd_test", 0, 2);
            for (byte[] element : zadd_test1) {
                System.out.println(new String(element));
            }
            Set<Tuple> zadd_test3 = template.zrangeWithScores("zadd_test", 0, 2);
            for (Tuple tuple : zadd_test3) {
                System.out.println(tuple.getElement() + " : " + tuple.getScore());
            }

            Set<byte[]> zadd_test2 = template.zrevrange("zadd_test", 0, 2);
            for (byte[] element : zadd_test2) {
                System.out.println(new String(element));
            }

            Set<Tuple> zadd_test4 = template.zrevrangeWithScores("zadd_test", 0, 2);
            for (Tuple tuple : zadd_test4) {
                System.out.println(tuple.getElement() + " : " + tuple.getScore());
            }

            Set<Tuple> zadd_test = template.zrevrangeByScoreWithScores("zadd_test", 10d, 0d, 0, 3);
            for (Tuple tuple : zadd_test) {
                System.out.println(tuple.getElement() + " : " + tuple.getScore());
            }
            template.zrem("zadd_test", "server4");
            zadd_test = template.zrevrangeByScoreWithScores("zadd_test", 10d, 0d, 0, 3);
            for (Tuple tuple : zadd_test) {
                System.out.println(tuple.getElement() + " : " + tuple.getScore());
            }

        } catch (Exception e) {
            throw e;
        }
    }


    @Test
    public void testSetNx() {
        for (int i = 0; i < 2; i++) {
            Long testsetnx = template.setnx("testsetnx", i);
            System.out.println("index i: " + i + "  ,set value result is :" + testsetnx);
        }
    }

    @Test
    public void testSetRem() {
        try {
            List<String> list = Lists.newArrayList("server1", "server2", "server3", "server4", "server5", "server6");
            for (String value : list) {
                template.sadd("sadd_test", value);
            }

            Long srem = template.srem("sadd_test", "server1");

            System.out.println("delete members : " + srem);

            Set<String> sadd_test = template.smembers("sadd_test", String.class);
            for (String tuple : sadd_test) {
                System.out.println(tuple);
            }
        } catch (Exception e) {
            throw e;
        }
    }


}
