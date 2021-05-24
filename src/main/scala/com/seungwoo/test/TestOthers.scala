package com.seungwoo.test

import com.google.gson.Gson
import com.seungwoo.utils.{Flink_MysqlUtils, Flink_RedisUtils}
import redis.clients.jedis.Jedis


object TestOthers {
  def main(args: Array[String]): Unit = {
    case class UserBehiver(id:String,name:String,time:Long)

    val gson = new Gson()
    println(gson.toJson(UserBehiver))

  }
}