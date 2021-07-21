package com.seungwoo.project.highlevelpro.EcommerceDataAnalyse

import java.time.Duration
import java.util
import java.util.Map

import com.google.gson.Gson
import org.apache.flink.api.scala._
import com.seungwoo.bean.UserBehavior
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

object RealTimeHotGoodsStatistics {
  //	每隔5分钟输出最近1小时内点击量最多的前N个商品
  //鉴于数据量有限：每隔1s中输出最近5s的数据量
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //数据样例：543462,1715,1464116,pv,1511658000

    val kafkaConsumerPro = new java.util.Properties()
    kafkaConsumerPro.setProperty("bootstrap.servers", "localhost:9092")
    kafkaConsumerPro.setProperty("group.id", "goods")

    val myFKConsumer = new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), kafkaConsumerPro)
    myFKConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(myFKConsumer)
    val userBehaviorStream: DataStream[UserBehavior] = kafkaStream
      .map(
        data => {
          new Gson().fromJson(data, classOf[UserBehavior])
        }
      )
    //增加水印策略：定期周期获取
    val boo: WatermarkStrategy[UserBehavior] = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
      new SerializableTimestampAssigner[UserBehavior] {
        override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = {
          element.timestamp
        }
      }
    )

    //滑动窗口：窗口大小1hour，滑动步长5min
    //过滤点击行为数据
    //按照商品id分组
    //求和
    //倒序排列取前N位
    userBehaviorStream
      .assignTimestampsAndWatermarks(boo)
      .filter(_.behavior == "pv")
      .map(
        data => {
          (data.categoryID, 1L)
        }
      ).keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
      .process(new MyProcessWindowFunction)
      .print("=>")

    env.execute()
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[(Long, Long), ArrayBuffer[(Long, Long)], Long, TimeWindow] {
    var goodsIDCountsMapState: MapState[Long, Long] = _

    override def open(parameters: Configuration): Unit = {
      val gicmsDesc = new MapStateDescriptor[Long, Long]("gicms", classOf[Long], classOf[Long])
      goodsIDCountsMapState = getRuntimeContext.getMapState(gicmsDesc)
    }

    override def process(key: Long, context: Context, elements: Iterable[(Long, Long)], out: Collector[ArrayBuffer[(Long, Long)]]): Unit = {
      for (elem <- elements) {
        if (goodsIDCountsMapState.get(elem._1) != null) {
          goodsIDCountsMapState.put(elem._1, goodsIDCountsMapState.get(elem._1) + 1L)
        } else {
          goodsIDCountsMapState.put(elem._1, 1L)
        }
      }

      //假定不存在有两种商品Id对应的统计数量相等。
      val goosIDAndCountsIterator: util.Iterator[Map.Entry[Long, Long]] = goodsIDCountsMapState.iterator()
      val goodsIdAndCountsBuffer: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long, Long)]()

      while (goosIDAndCountsIterator.hasNext) {
        val keyAndValueMap: Map.Entry[Long, Long] = goosIDAndCountsIterator.next()
        val key: Long = keyAndValueMap.getKey
        val value: Long = keyAndValueMap.getValue
        if (key != null && keyAndValueMap != null) {
          goodsIdAndCountsBuffer.append((key, value))
        }
      }
      //      println("goodsIdAndCountsBuffer:==>" + goodsIdAndCountsBuffer.toList)
      val resultsArrayBuffer: ArrayBuffer[(Long, Long)] = goodsIdAndCountsBuffer
        .sortWith(_._2 > _._2)
        .take(3)

      out.collect(resultsArrayBuffer)

      goodsIDCountsMapState.clear()
    }
  }
//==============待修复
}
