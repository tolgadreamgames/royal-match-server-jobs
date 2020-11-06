package com.dreamgames.royalmatchserverjobs.redis;

import com.dreamgames.royalmatchserverjobs.util.ServerProperties;
import com.dreamgames.royalmatchserverjobs.util.Tuple;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RedisManager {
    public static void fillLeaderboard(Map<String, List<Tuple<Long, Double>>> leaderboard) {
        var expireInSeconds = (int) TimeUnit.DAYS.toSeconds(30);


        if(ServerProperties.IS_DEV) {
            var jedis = getSingleRedis();
            for(Map.Entry<String, List<Tuple<Long, Double>>> entry : leaderboard.entrySet()) {
                var key = "leaderboard_players_" + entry.getKey();
                jedis.expire(key, expireInSeconds);

                var list = entry.getValue();
                for (Tuple<Long, Double> tuple : list) {
                    jedis.zadd(key, tuple.right, String.valueOf(tuple.left));
                }
            }
        }else {
            var jedis = getClusterRedis();
            for(Map.Entry<String, List<Tuple<Long, Double>>> entry : leaderboard.entrySet()) {
                var key = "leaderboard_players_" + entry.getKey();
                jedis.expire(key, expireInSeconds);

                var list = entry.getValue();
                for (Tuple<Long, Double> tuple : list) {
                    jedis.zadd(key, tuple.right, String.valueOf(tuple.left));
                }
            }
        }
    }

    private static Jedis getSingleRedis() {
        return new Jedis(ServerProperties.getString("redis.hostname"), ServerProperties.getInt("redis.port"));
    }

    private static JedisCluster getClusterRedis() {
        return new JedisCluster(new HostAndPort(ServerProperties.getString("redis.hostname"), ServerProperties.getInt("redis.port")));
    }
}
