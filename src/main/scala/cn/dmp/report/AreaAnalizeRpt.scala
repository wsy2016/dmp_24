package cn.dmp.report

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object AreaAnalizeRpt {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        """
          |cn.dmp.report.ProCityRpt
          |参数：
          | logInputPath
        """.stripMargin
      )
      sys.exit(1)
    }

    val Array(logInputPath) = args

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sparkContext)
    val df = sQLContext.read.parquet(logInputPath)
    df.registerTempTable("log")

    val result = sQLContext.sql(
      """
        |select
        |provincename, cityname,
        |sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) 有效请求,
        |sum(case when requestmode=1 and processnode =3 then 1 else 0 end) 广告请求,
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 then 1 else 0 end) 参与竞价数,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) 竞价成功数,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) 展示数,
        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) 点击数,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*adpayment/1000 else 0 end) 广告成本,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*winprice/1000 else 0 end) 广告消费
        |from log
        |group by provincename, cityname
      """.stripMargin)

    result.show()
  }
}
