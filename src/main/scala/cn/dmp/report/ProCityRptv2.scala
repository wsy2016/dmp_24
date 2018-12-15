package cn.dmp.report

import java.util.Properties

import cn.dmp.utils.ConfUtils
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object ProCityRptv2 {
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
    var rs = sQLContext.sql("select provincename, cityname, count(*) ct from log group by provincename, cityname order by ct desc")
    val properties = new Properties()
    properties.setProperty("user", ConfUtils.getString("jdbc.user"))
    properties.setProperty("password", ConfUtils.getString("jdbc.password"))
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    val url = ConfUtils.getString("jdbc.url")
    val tableName = ConfUtils.getString("jdbc.tableName")
    rs.coalesce(1).write.mode(SaveMode.Overwrite) jdbc(url, tableName, properties)
    sparkContext.stop()
  }

}
