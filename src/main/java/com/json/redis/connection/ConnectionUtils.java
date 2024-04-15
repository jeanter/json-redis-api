package com.json.redis.connection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.json.redis.Constants;

/**
 * Created by   on 12/21/16.
 */
public class ConnectionUtils {

    public static void releaseConnection(Connection connection) {
        if (connection != null) {
            connection.close();
        }
    }

    public static List<RedisServer> getRedisServerList(String ipAddressList) {
        if (ipAddressList == null) {
            return null;
        }
        String[] ipAddressArray = ipAddressList.split(Constants.COMMA);
        if (ipAddressArray == null || ipAddressArray.length == 0) {
            return null;
        }
        List<RedisServer> list = new ArrayList<RedisServer>();
        for(String ipAddress : ipAddressArray) {
            String[] array = ipAddress.split(Constants.COLON);
            if (array == null || array.length !=2) {
                continue;
            }
            RedisServer server = new RedisServer();
            server.setHost(array[0]);
            try{
                server.setPort(Integer.parseInt(array[1]));
            }catch (Exception e) {
                continue;
            }
            list.add(server);
        }
        if (list.size() == 0) {
            return null;
        }
        Collections.sort(list);
        return list;
    }

}
