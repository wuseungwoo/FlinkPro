package com.seungwoo.datastreamAPI.source

import java.sql.{Connection, ResultSet, Statement}

import com.seungwoo.utils.Flink_MysqlUtils
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


//这里可以考虑使用Flink CDC！
object Flink_MysqlSource {
  def main(args: Array[String]): Unit = {
    //目的为获取mysql的维表中全量全量数据，目前可以全部打印出来
    //这里是想获取到一个datastream
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val mysqlStream: DataStream[String] = env.addSource(new MysqlSource()).setParallelism(1)

    val result: DataStream[(String, Int)] = mysqlStream.map(
      data => {
        ("a", 1)
      }
    ).keyBy(0).sum(1)
    result.map(_._2)
    mysqlStream.map(
      data=>{
        ("a",data.getBytes().length)
      }
    ).keyBy(0).sum(1).map(_._2).print()
      env.execute("load mysql table`s:whfwd data to print")
  }

  class MysqlSource extends SourceFunction[String] {
    var con: Connection = _
    var ps: Statement = _
    var resultSet: ResultSet = _

    override def run(ctx: SourceFunction.SourceContext[String]) = {
      //调用Flink_MysqlUtils工具类中获取链接的方法
      con=Flink_MysqlUtils.getMysqlConnection()

      val sql: String =
        """
          |select * from whfwd
        """.stripMargin

      ps = con.createStatement()

      resultSet = ps.executeQuery(sql)

      var message: String = " "

      while (resultSet.next()) {
        val fund_account: String = resultSet.getString("fund_account")
        val open_date: String = resultSet.getString("open_date")
        val xz: String = resultSet.getString("xz")
        val zt: String = resultSet.getString("zt")

        message =
          s"""
             |"账户："$fund_account"/日期："$open_date"/限制："$xz"/状态："$zt
          """.stripMargin

        ctx.collect(message)
      }
    }

    var running: Boolean = true

    override def cancel(): Unit = {
      running = false
      if(resultSet != null){
        resultSet.close()
      }

      if(ps != null){
        Flink_MysqlUtils.close(ps)
      }
      if(con != null){
        Flink_MysqlUtils.close(con)
      }
    }
  }
}



