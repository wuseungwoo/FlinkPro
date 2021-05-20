package com.seungwoo.datastreamAPI.source

import com.seungwoo.utils.Flink_RedisUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import redis.clients.jedis.{Jedis, JedisPool}

object Flink_RedisSource {
  def main(args: Array[String]): Unit = {
    //读redis数据作为维度数据获得datastream打印输出
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val redisStream: DataStream[String] = env.addSource(new RedisSource)
    redisStream.print()
    env.execute("read data from redis")
  }

  class RedisSource extends SourceFunction[String] {
    var jedis: Jedis = null
    var jedispoll:JedisPool = null

    override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
      jedispoll = Flink_RedisUtils.getJedis()._2
      jedis = Flink_RedisUtils.getJedis()._1
      val avalue: String = jedis.get("a")
      val bvalue: String = jedis.get("b")

      val message: String = avalue + " -love- " + bvalue

      sourceContext.collect(message)
    }

    var running: Boolean = true

    override def cancel(): Unit = {
      running = false
      Flink_RedisUtils.closeJedis(jedis,jedispoll)
    }
  }

}
