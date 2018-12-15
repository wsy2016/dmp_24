package cn.dmp.report

import org.apache.spark.{SparkConf, SparkContext}

object ProCityRptRDD {
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

    val Array(logInputPath,resultOutputPath) = args

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.textFile(logInputPath)
      .map(_.split(","))
      .filter(_.length >= 85)
      .map(arr => ((arr(24), arr(25)), 1))
      .reduceByKey(_ + _)
      .map(t=>t._1._1+","+t._1._2+","+t._2)
      .saveAsTextFile(resultOutputPath)
    sparkContext.stop()
  }

}
