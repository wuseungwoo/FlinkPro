package com.seungwoo.datastreamconnector.sink

import java.net.{InetAddress, InetSocketAddress}
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.elasticsearch7.{ElasticsearchSink, RestClientFactory}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}

object Flink_ESSink {
  def main(args: Array[String]): Unit = {
    //Elasticsearch 在项目中主要用于实时数仓,用户画像
    //因此掌握Elasticsearch的数据写入是非常有必要的
    //Elasticsearch在项目中一般承担：分布式多用户能力的全文搜索引擎，关键字查询的全文搜索功能
    //Flink+kafka+ES->用户画像
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val topic: String = "flink_kafka"
    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "localhost:9092")

    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](
      topic, new SimpleStringSchema(), properties
    ))

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

    val esSinkBuilder: ElasticsearchSink.Builder[String] = new ElasticsearchSink.Builder[String](httpHosts, new ElasticsearchSinkFunction[String] {
      override def process(element: String, runtimeContext: RuntimeContext, indexer: RequestIndexer): Unit = {
        val json = new java.util.HashMap[String, String]
        json.put("data", element)

        val rqst: IndexRequest = Requests.indexRequest
          .index("my-index")
          .source(json)

        indexer.add(rqst)
      }
    })
    esSinkBuilder.setBulkFlushMaxActions(1)

    esSinkBuilder.setRestClientFactory(new RestClientFactory {
      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
        restClientBuilder.setDefaultHeaders()
        restClientBuilder.setRequestConfigCallback()
        restClientBuilder.setRequestConfigCallback()
      }
    })
    kafkaStream.addSink(esSinkBuilder.build())
  }
}
