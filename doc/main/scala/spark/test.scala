package spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName("tt").setMaster("local")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val df = hc.sql("select * from qfbap_ods.ods_code_category")
    df.show()
  }
}
