package com.seungwoo.test

import com.seungwoo.utils.{Flink_MysqlUtils, Flink_RedisUtils}
import redis.clients.jedis.Jedis


object TestOthers {
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = Flink_RedisUtils.getJedis()._1

    jedis.set("a","wusuengwoo")

  }
}