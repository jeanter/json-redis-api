package com.json.redis.connection;

/**
 * Created by   on 12/21/16.
 */
public class RedisServer implements Comparable<RedisServer> {

    private String host;
    private Integer port;

    public RedisServer() {
        this("localhost");
    }

    public RedisServer(String host) {
        this(host, 6379);
    }

    public RedisServer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public int compareTo(RedisServer o) {
        if (o == null) {
            return -1;
        }
        if (o.getHost() == null) {
            return -2;
        }
        if (o.getPort() == null) {
            return -3;
        }
        if (o.getHost().equals(host) && o.getPort() == port) {
            return 0;
        }


        return 0;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }


}
