package com.atguigu;

import redis.clients.jedis.Jedis;

/**
 * @author wzy
 * @create 2020-07-13 19:48
 */
public class TestJedis {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.119.202", 6379);
        //执行ping命令
        String ping = jedis.ping();

        System.out.println(ping);

        jedis.set("first","ppp");
        jedis.lpush("secondList","1","2");
//关闭连接
        jedis.close();


    }
}
