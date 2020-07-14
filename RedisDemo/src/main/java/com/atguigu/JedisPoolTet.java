package com.atguigu;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

/**
 * @author wzy
 * @create 2020-07-13 20:03
 */
public class JedisPoolTet {
    public static void main(String[] args) {
        //声明Linux服务器IP地址
        String host = "192.168.119.202";

//声明Redis端口号
        int port = 6379;

//创建连接池对象
        JedisPool jedisPool = new JedisPool(host, port);

//获取Jedis对象连接Redis
        Jedis jedis = jedisPool.getResource();

//执行具体操作
        String ping = jedis.ping();

        System.out.println(ping);

//关闭连接
        //jedisPool.close();
        Jedis jedis1 = jedisPool.getResource();
        String ping1 = jedis1.ping();

        System.out.println(ping1);
        jedis.close();
    }
}
