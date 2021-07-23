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
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
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
    env.setParallelism(1)

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
    val boo: WatermarkStrategy[UserBehavior] = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
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
    //这里使用到了全窗口和全窗口函数，相比较keyBY后的操作方式效率可能会慢点
    userBehaviorStream
      .assignTimestampsAndWatermarks(boo)
      .filter(_.behavior == "pv")
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
      .process(new MyProceeAllWindowFunction)
      .map(_.toList)
      .print()

    env.execute("HotGoods")
  }

  class MyProceeAllWindowFunction extends ProcessAllWindowFunction[UserBehavior, ArrayBuffer[(Long, Long)], TimeWindow] {
    var goodsCounts: MapState[Long, Long] = _

    override def open(parameters: Configuration): Unit = {
      val hotgoodsDesc = new MapStateDescriptor[Long, Long]("hotgoodsmapstatedesc", classOf[Long], classOf[Long])
      goodsCounts = getRuntimeContext.getMapState(hotgoodsDesc)
    }

    override def process(context: Context, elements: Iterable[UserBehavior], out: Collector[ArrayBuffer[(Long, Long)]]): Unit = {
      for (elem <- elements) {
        if (goodsCounts.get(elem.categoryID) != null) {
          goodsCounts.put(elem.categoryID, goodsCounts.get(elem.categoryID) + 1L)
        } else {
          goodsCounts.put(elem.categoryID, 1L)
        }
      }

      val goodsCountsIter: util.Iterator[Map.Entry[Long, Long]] = goodsCounts.iterator()
      val goodsCountsArrayBuffer = new ArrayBuffer[(Long, Long)]()
      while (goodsCountsIter.hasNext) {
        val goodsCountsMap: Map.Entry[Long, Long] = goodsCountsIter.next()
        val keys: Long = goodsCountsMap.getKey
        val values: Long = goodsCountsMap.getValue

        if (keys != null && values != null) {
          goodsCountsArrayBuffer.append((keys, values))
        }
      }
      val resultsArrayBuffer: ArrayBuffer[(Long, Long)] = goodsCountsArrayBuffer
        .sortWith(_._2 > _._2)
        .take(3)

      //水位线到达窗口关闭时间输出
      out.collect(resultsArrayBuffer)

      //窗口关闭的时候清除状态内维护的值
      goodsCounts.clear()
    }
  }

}
