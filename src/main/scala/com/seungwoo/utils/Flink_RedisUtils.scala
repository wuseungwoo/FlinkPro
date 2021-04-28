package com.seungwoo.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object Flink_RedisUtils {
  //配置redis
  val host: String = "127.0.0.1"
  val port: Int = 6379
  var jedispool: JedisPool = null
  var jedis:Jedis = null


  //单例模式获取jedispool链接线程池
  def getJedis(): (Jedis,JedisPool) = {
    if (jedispool == null) {
      this.synchronized{
        if (jedispool == null) {
          val jedisPoolConfig = new JedisPoolConfig

          jedisPoolConfig.setMaxIdle(30)

          jedisPoolConfig.setMaxTotal(100)

          jedisPoolConfig.setMaxWaitMillis(100 * 1000)

          jedisPoolConfig.setTestOnBorrow(true)

          jedispool = new JedisPool(jedisPoolConfig,host,port)

          jedis = jedispool.getResource
        }
      }
    }
    (jedis,jedispool)
  }

  //关闭jedis链接
  def closeJedis(jedis: Jedis,jedisPool: JedisPool): Unit ={
    if (jedis != null){
      jedis.close()
      jedisPool.close()
    }
  }


}
