package caitong.etl

import java.text.SimpleDateFormat
import java.util.Calendar

object ads_inf_risk_fund_info_dd {
  def main(args: Array[String]): Unit = {
    //定义时间变量
    var update_date = " "
    //更新日期，分区日期
    var execute_date = " "
    //执行日期
    var start_time = " " //查询开始时间（年月日 时分秒）
    var end_time = " " //查询结束时间（年月日 时分秒）

    val cal = Calendar.getInstance() //日历实例
    //定义系统变量
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hqlContext = new HiveContext(sc)
    hqlContext.sql("set hive.exec.stagingdir=/tmp/staging/jfpt/.hive-staging")

    //日期处理逻辑
    /*最多支持一个日期参数，即统计日期格式yyyyMMdd
    传入参数跑指定日期前一天的数据
    不传参数跑当前日期前一天的数据
     */

    val input_args = args.length
    input_args match {
      case 1 => {
        //输入一个日期参数，则执行输入日期的结果/不过查询历史数据意义不大(客户持仓表每日全量刷新最新状态)
        update_date = args(0)
        //更新日期=输入日期
        val yesdate = new SimpleDateFormat("yyyyMMdd").parse(update_date)
        cal.setTime(yesdate)
        end_time = new SimpleDateFormat("yyyy-MM-dd 08:00:00").format(cal.getTime) //后一天08：00
        cal.add(Calendar.DATE, -1) //执行日期=输入日期-1
        execute_date = new SimpleDateFormat("yyyyMMdd").format(cal.getTime)
        start_time = new SimpleDateFormat("yyyy-MM-dd 09:30:00").format(cal.getTime) //前一天09：30
        System.out.print("输入日期参数正确，更新日期：" + update_date + "；查询时间：" + start_time + "~" + end_time)
      }
      case _ => {
        //未输入参数或参数不合法
        update_date = new SimpleDateFormat("yyyyMMdd").format(cal.getTime) //更新日期=当前日期
        end_time = new SimpleDateFormat("yyyy-MM-dd 08:00:00").format(cal.getTime) //今天08：00
        cal.add(Calendar.DATE, -1)
        execute_date = new SimpleDateFormat("yyyyMMdd").format(cal.getTime) //执行日期=当前日期-1
        start_time = new SimpleDateFormat("yyyy-MM-dd 09:30:00").format(cal.getTime) //昨天09：30
        cal.add(Calendar.DATE, -3)
        System.out.println("更新日期：" + update_date + "；查询时间：" + start_time + "~" + end_time)
      }
    }
    println("更新日期：" + update_date + ";查询时间：" + start_time + "~" + end_time)

    //先清空数据后插入
    hqlContext.sql(
      """
        |truncate table report.ads_inf_risk_fund_info_dd
      """.stripMargin)

    //获取发布预警公告的股票代码(按照已提供的sql去执行查询)时间范围：between start_time and end_time
    val risk_fund_bulletins = hqlContext.sql(
      s"""
         |select a.SecuCode secucode,
         |       c.DisclName disclname,
         |       B.BULLETINDATE bulletindate
         |  from jy.mf_nottextannouncement_1month t,
         |       jy.SecuMain A,
         |       jy.MF_InterimBulletin b,
         |       (select c.innercode,
         |               c.disclname,
         |               ROW_NUMBER() OVER(PARTITION BY C.INNERCODE ORDER BY C.EFFECTIVEDATE DESC) RN
         |          from jy.MF_FundProdName c
         |         where c.infotype = 1) as c
         | where t.Category = 99
         |   and T.InfoTitle like '%提示及停复牌公告%'
         |   and T.InnerCode = A.InnerCode
         |   and T.InterBulletinID = b.ID
         |   and t.InnerCode = c.innercode
         |   and B.BULLETINDATE >= '$start_time'
         |   and B.BULLETINDATE <='$end_time'
         |   and c.rn = 1
      """.stripMargin)
    risk_fund_bulletins.registerTempTable("risk_fund_bulletins")
    hqlContext.cacheTable("risk_fund_bulletins")


    //这里存在使用到的上游表dc.dws_ast_acc_stockvalue_di中分区更新不及时的bug
    //因此需要对dc.dws_ast_acc_stockvalue_di的分区取最新分区而不是昨日分区的逻辑判断

    //昨日分区中有数据就用昨日分区，否则就用dc.dws_ast_acc_stockvalue_di最新分区的数据
    val judge_table: DataFrame = hqlContext.sql(
      s"""
         |select busi_date from dc.dws_ast_acc_stockvalue_di where busi_date = '$execute_date' limit 1
      """.stripMargin)

    judge_table.registerTempTable("judge_table")

    val data: Array[Row] = judge_table.select("busi_date").collect()

    val have_data: Int = data.length

    if (have_data == 0) {
      val cur_month: String = execute_date.substring(0,6)
      val cur_month_first: String = cur_month+"01"
      //断定至少月初以后肯定是有数据的：主要过滤的目的是为了加快查询速度
      val max_busi_date = hqlContext.sql(
        s"""
           |SELECT
           |	max(a.busi_date) max_busi_date
           |FROM
           |	(
           |		SELECT
           |			busi_date
           |		FROM
           |			dc.dws_ast_acc_stockvalue_di
           |		WHERE
           |			busi_date > '$cur_month_first'
           |		GROUP BY
           |			busi_date
           |	) AS a
        """.stripMargin)
      max_busi_date.registerTempTable("max_busi_date")

      val array: Array[Row] = max_busi_date.select("max_busi_date").collect()
      for (elem <- array) {
        //在昨日分区无数据的情况下，将最新分区赋值给execute_date
        execute_date = elem.toString()
      }
    }


    val final_table = hqlContext.sql(
      s"""
         |select
         | from_unixtime(unix_timestamp(),'yyyyMMdd') update_date,
         | s.client_id client_id,
         | ct.client_name client_name,
         | if(co.mobile_tel is not null,co.mobile_tel,co.home_tel) client_phone_num,
         | r.bulletindate publish_date,
         | r.secucode fund_code,
         | r.disclname fund_name,
         | concat('尊敬的投资者您好！您持有的',r.disclname,'（基金代码：',r.secucode,'）','目前二级市场交易价格较基金份额参考净值的溢价幅度较高。根据交易所公告，为了保护基金份额持有人利益，',r.disclname,'将于',date_format(r.bulletindate,'yyyy'),'年',date_format(r.bulletindate,'MM'),'月',date_format(r.bulletindate,'dd'),'日','开市起至当日10:30停牌，自',date_format(r.bulletindate,'yyyy'),'年',date_format(r.bulletindate,'MM'),'月',date_format(r.bulletindate,'dd'),'日','10:30起复牌。敬请您关注相关风险提示公告，注意投资风险。如有疑问可咨询财通证券客服电话95336。') message
         |from
         | (select
         |    secucode,
         |    disclname,
         |    bulletindate
         |  from risk_fund_bulletins
         |  ) as r
         | inner join
         | (select
         |   client_id,
         |   prod_code
         |  from dc.dws_ast_acc_stockvalue_di
         |  where busi_date = '$execute_date'
         |  and (current_amount+correct_amount) <> 0
         |  group by client_id,prod_code
         |  ) as s on r.secucode = s.prod_code
         | left join uf20.clientinfo co on s.client_id = co.client_id
         | left join uf20.client ct on s.client_id = ct.client_id
        """.stripMargin)
    final_table.registerTempTable("final_table")

    //插入数据
    hqlContext.sql(
      s"""
         |insert into table report.ads_inf_risk_fund_info_dd
         |select
         | *
         |from final_table
        """.stripMargin)

    hqlContext.uncacheTable("risk_fund_bulletins")
    sc.stop()
  }
}