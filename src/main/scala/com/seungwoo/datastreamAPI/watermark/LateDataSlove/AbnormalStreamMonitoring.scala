package com.seungwoo.datastreamAPI.watermark.LateDataSlove

import com.google.gson.Gson
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
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
    val gson = new Gson()
    val userTransactionStream: DataStream[UserTransaction] = kafkaStream.map(
      data => {
        gson.fromJson(data, classOf[UserTransaction])
      }
    )
    //1.可能存在无交易额或者无转账的情况,此时应对这两个值进行判断后过滤不参与统计
    val haveTransactionStream: DataStream[UserTransaction] = userTransactionStream.filter(
      data => {
        data.transaction_amount != null && data.transfer_accounts != null
      }
    )

    //数据为乱序数据，延迟时间不超过10s（描述数据的混乱程度）
    //允许接收迟到10s的数据
    //2.可能存在单条数据直接超过 交易额>5万以上的，或者转账20w以上的 直接进入大额交易记录历史名单（需要去重）
    //将符合要求的这部分流直接侧输出到大额交易记录历史名单

    val uTws: WatermarkStrategy[UserTransaction] = new WatermarkStrategy[UserTransaction] {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[UserTransaction] = {
        new TransactionWatermarkStrategy
      }
    }.withTimestampAssigner(new SerializableTimestampAssigner[UserTransaction] {
      override def extractTimestamp(t: UserTransaction, l: Long): Long = {
        t.time
      }
    })
    haveTransactionStream
      .assignTimestampsAndWatermarks(uTws)
      .process(new ProcessFunction[UserTransaction,UserTransaction] {
        override def processElement(value: UserTransaction, ctx: ProcessFunction[UserTransaction, UserTransaction]#Context, out: Collector[UserTransaction]): Unit = {
          if(value.transaction_amount>50000L || value.transfer_accounts > 200000L){

          }
        }
      })


    //3.分流完毕后的数据只剩下：交易额>5万以上 且 转账20w以上的


    //需求: 采集监控交易流数据，将交易额累加值高于5w的值输出到side output transaction

    //需求: 采集监控交易流数据，将转账金额累加值高于20w的值输出到side output transfer

    //sink：写出外部存储形成大额交易记录历史名单：形成大额交易监控对象（后续做重点关注）->写入之前需要去重
    //考虑使用内嵌RocksDB状态后端去重


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