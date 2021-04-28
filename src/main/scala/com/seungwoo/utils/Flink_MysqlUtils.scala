package com.seungwoo.utils

import java.sql._
import java.util.Properties

import org.apache.commons.dbcp2.BasicDataSource

object Flink_MysqlUtils {
  //  val prop: Properties = new Properties()
  //  //配置文件获取配置
  //  def getMysqlProperties():Properties = {
  //    prop.load(Flink_MysqlUtils.getClass.getClassLoader.getResourceAsStream("com/seungwoo/utils/mysql.properties"))
  //    prop
  //  }

  //  val driver:String = Flink_MysqlUtils.getMysqlProperties().getProperty("driver")
  //  val username:String = Flink_MysqlUtils.getMysqlProperties().getProperty("username")
  //  val password:String = Flink_MysqlUtils.getMysqlProperties().getProperty("password")
  //  val url:String = Flink_MysqlUtils.getMysqlProperties().getProperty("url")


  var driver = "com.mysql.jdbc.Driver"
  var url = "jdbc:mysql://localhost:3306/test"
  var username = "root"
  var password = "root"

  var source: BasicDataSource = null
  var con: Connection = null


////  通过new BasicDataSource获取源对象
//    source = new BasicDataSource
////  设置四件套
//    source.setDriverClassName(driver)
//    source.setUsername(username)
//    source.setPassword(password)
//    source.setUrl(url)
//
////  设置连接池的一些参数//设置连接池的一些参数
//    source.setInitialSize(10)
//    source.setMaxTotal(50)
//    source.setMinIdle(2)
//  def getMysqlConnection(): Connection ={
//    source.getConnection
//  }

  //但凡是获取链接就必须得使用连接池，且必须得使用单例模式获取连接池和链接
  //单例模式获取链接(方法上加@throws抛出异常)
  @throws(classOf[SQLException])
  def getMysqlConnection():Connection= {
    if (source == null) {
      this.synchronized {
        if (source == null) {
          source = new BasicDataSource

          source.setDriverClassName(driver)
          source.setUsername(username)
          source.setPassword(password)
          source.setUrl(url)

          source.setInitialSize(10)
          source.setMaxTotal(50)
          source.setMinIdle(2)

          con = source.getConnection
        }
      }
    }
    con
  }

  //关闭资源
  @throws(classOf[SQLException])
  def close(con: Connection): Unit = {
    if (con != null) {
      con.close()
    }
  }

  def close(ps: Statement): Unit = {
    if (ps != null) {
      ps.close()
    }
  }

  def close(con: Connection, ps: Statement): Unit = {
    if (con != null && ps != null) {
      ps.close()
      con.close()
    }
  }
}

