package com.seungwoo.datastreamAPI.watermark.LateDataSlove

import com.google.gson.Gson
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

object AbnormalStreamMonitoring {
  def main(args: Array[String]): Unit = {
    //transaction amount :交易金额
    //在金融领域会对交易数据流进行大额交易监控：可疑交易报告
    /*
    可疑交易报告；出处：中国人民银行令，金融机构大额交易和可疑交易报告管理办法
    当证券公司有合理理由怀疑客户或者其交易对手、资金或者其他资产与名单相关的，
    应当在立即向中国反洗钱监测分析中心提交可疑交易报告的同时，
    以电子形式或书面形式向所在地中国人民银行或者其分支机构报告，
    并按照相关主管部门的要求依法采取措施。
     */

    //单日交易额>5万以上的，或者转账20w以上的
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //数据源是通过java生成的json进入kafka
    val per = new java.util.Properties()

    per.setProperty("bootstrap.servers", "localhost:9092")
    per.setProperty("group.id", "side_out_put_data")

    val sideFKConsumer = new FlinkKafkaConsumer[String]("flink_kafka", new SimpleStringSchema(), per)

    //这里简单设置下，后面会在flink的容错机制中做详细介绍；
    //参考博客：
    /*
    1.  https://www.sohu.com/a/168546400_617676
    2.  https://cpeixin.cn/2020/11/21/Flink%E6%B6%88%E8%B4%B9Kafka%E4%BB%A5%E5%8F%8A%E5%8F%82%E6%95%B0%E8%AE%BE%E7%BD%AE/
     */
    sideFKConsumer.setStartFromLatest()

    val kafkaStream: DataStream[String] = env.addSource(sideFKConsumer)

    //转换为样例类的流
    val userTransactionStream: DataStream[UserTransaction] = kafkaStream.map(
    data => {
      val gson = new Gson()
      gson.fromJson(data, classOf[UserTransaction])
      }
    )
    //1.可能存在无交易额或者无转账的情况,此时应对这两个值进行判断后过滤不参与统计
    val haveTransactionStream: DataStream[UserTransaction] = userTransactionStream.filter(
      data => {
        data.transaction_amount != null && data.transfer_accounts != null
      }
    )


    val single: OutputTag[UserTransaction] = new OutputTag[UserTransaction]("a_single_transaction_achieve_the_goal"){}
    val calculate: OutputTag[UserTransaction] = new OutputTag[UserTransaction]("transaction_and_tansfer_need_to_calculate"){}
    val sidedStream: DataStream[UserTransaction] = haveTransactionStream
      .process(new ProcessFunction[UserTransaction, UserTransaction] {
        override def processElement(value: UserTransaction, ctx: ProcessFunction[UserTransaction, UserTransaction]#Context, out: Collector[UserTransaction]): Unit = {
          //2.可能存在单条数据直接超过 交易额>5万以上的，或者转账20w以上的 直接进入大额交易记录历史名单（需要去重）
          //将符合要求的这部分流直接侧输出到大额交易记录历史名单
          //单笔交易额满足条件，直接写入对应的单笔交易额直接进入名单的流中
          if (value.transaction_amount > 50000L || value.transfer_accounts > 200000L) {
            ctx.output(single, value)
          } else {
            //3.主流继续分流：交易额<5万以上 且 转账<20w以上的
            ctx.output(calculate, value)
          }
          //以上只是选择性输出了两条符合条件的流，实际上也可以把主流输出
          //out.collect(value)
        }
      })

    //需求: 采集监控交易流数据，将单条数据的交易金额高于5w或者单条数据的转账金额>20w的值输出到远程存储：redis
    //使用内嵌RocksDB状态后端去重后写入->直接存储：在学习到了flink状态编程的时候再处理
    //本次需求则利用redis的SET无序字符串集合的特性：实现去重
    //由于该集的唯一属性，它只添加一次，集合中的最大成员数为 232-1 个元素（超过 40 亿个元素）。

    //数据为乱序数据，延迟时间不超过10s（描述数据的混乱程度）
    //允许接收迟到10s的数据
    val uTws: WatermarkStrategy[UserTransaction] = new WatermarkStrategy[UserTransaction] {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[UserTransaction] = {
        new TransactionWatermarkStrategy
      }
    }.withTimestampAssigner(new SerializableTimestampAssigner[UserTransaction] {
      override def extractTimestamp(t: UserTransaction, l: Long): Long = {
        t.time
      }
    })

    val port:Int = 6379
    //这里顺带复习下redis的数据写入
    val host:String = "127.0.0.1"
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost(host).setPort(port).build()


    /*
    集合（set）是 Redis 数据库中的无序字符串集合。在 Redis 中，添加，删除和查找的时间复杂度是 O(1)
     */
    val singleStream: DataStream[UserTransaction] = sidedStream.getSideOutput(single)

    singleStream
      .assignTimestampsAndWatermarks(uTws)
      .keyBy(_.client_id)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .allowedLateness(Time.seconds(10))
      .reduce(new ReduceFunction[UserTransaction] {
        override def reduce(t: UserTransaction, t1: UserTransaction): UserTransaction = {
          t//始终按照id分组后 只保留前一个元素
        }
      })
      .addSink(new RedisSink[UserTransaction](config,new RedisMapper[UserTransaction] {
        override def getCommandDescription: RedisCommandDescription = {
          new RedisCommandDescription(RedisCommand.SADD)
        }

        override def getValueFromData(data: UserTransaction): String = {
          //大额交易的key：block_trade value:client_id
          data.client_id
        }

        override def getKeyFromData(data: UserTransaction): String = {
          "block_trade"
        }
      }))


    val calculateStream: DataStream[UserTransaction] = sidedStream.getSideOutput(calculate)

    val cld: OutputTag[UserTransaction] = new OutputTag[UserTransaction]("calculatestream_late_data"){}
    val redd: OutputTag[String] = new OutputTag[String]("reduced_data")
    val reddStream: DataStream[String] = calculateStream
      .assignTimestampsAndWatermarks(uTws)
      .keyBy(_.client_id)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .allowedLateness(Time.seconds(10))
      .sideOutputLateData(cld)
      .reduce(new ReduceFunction[UserTransaction] {
        override def reduce(t: UserTransaction, t1: UserTransaction): UserTransaction = {
          UserTransaction(t.client_id, t.client_name, t.transaction_amount + t1.transaction_amount, t.transfer_accounts + t1.transfer_accounts, t1.time)
        }
      })
      .process(new ProcessFunction[UserTransaction, String] {
        override def processElement(value: UserTransaction, ctx: ProcessFunction[UserTransaction, String]#Context, out: Collector[String]): Unit = {
          if (value.transaction_amount > 50000L || value.transfer_accounts > 200000L) {
            ctx.output(redd, value.client_id)
          }
        }
      })
      .getSideOutput(redd)
    reddStream.addSink(new RedisSink[String](config,new RedisMapper[String] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.SADD)
      }

      override def getValueFromData(data: String): String = {
        data
      }

      override def getKeyFromData(data: String): String = {
        "block_trade"
      }
    }))
    env.execute("block_trade_Monitoring")
  }

  class TransactionWatermarkStrategy extends WatermarkGenerator[UserTransaction] {
    val maxOutOfOrderness: Long = 10000L //10s乱序程度

    var currentMaxTimestamp: Long = _

    override def onEvent(t: UserTransaction, eventtimestamps: Long, watermarkOutput: WatermarkOutput): Unit = {
      currentMaxTimestamp = Math.max(currentMaxTimestamp, eventtimestamps)

    }

    override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
      watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))

    }
  }

}

//样例类中应该包含有交易金额和转账金额
case class UserTransaction(client_id: String, client_name: String, transaction_amount: Long, transfer_accounts: Long, time: Long)