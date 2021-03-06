package cn.dmp.report

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ProCityRpt {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |cn.dmp.report.ProCityRpt
          |参数：
          | logInputPath
          | resultOutputPath
        """.stripMargin
      )
      sys.exit(1)
    }

    val Array(logInputPath, resultOutputPath) = args

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sparkContext)
    val df = sQLContext.read.parquet(logInputPath)
    df.registerTempTable("log")
    var rs = sQLContext.sql("select provincename, cityname, count(*) ct from log group by provincename, cityname order by ct desc")
    val hadoopConfiguration = sparkContext.hadoopConfiguration
    val fileSystem = FileSystem.get(hadoopConfiguration)
    val path = new Path(resultOutputPath)
    if (fileSystem.exists(path)){
      fileSystem.delete(path)
    }
    rs.coalesce(1).write.json(resultOutputPath)

    sparkContext.stop()
  }
}
