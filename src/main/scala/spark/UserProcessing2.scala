package spark

import java.util.Properties

import conf.ConfigManager
import constant.Constants
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import utils.SparkUtils

object UserProcessing2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf()
      .setAppName(Constants.SPARK_APP_NAME_USER)
    //SparkUtils.setMaster(conf)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val ssc = new SQLContext(sc)

    /**
      * args：作业调度时传进来的参数，其中：
      * args(0)：将要抽取到该表的表名
      * args(1)：日期条件
      */

    // 加载相应的sql语句
    val sql = ConfigManager.getProperty(args(0))
    val df = sqlContext.sql(sql)
    //df.show(20)
//    val prop = new Properties()
//    prop.put("user", ConfigManager.getProperty("jdbc.user"))
//    prop.put("password", ConfigManager.getProperty("jdbc.password"))
//    prop.put("driver", ConfigManager.getProperty("jdbc.driver"))
//    val str: String = ConfigManager.getProperty("jdbc.url")
//    df.write.mode("append").jdbc(str, "dm_user_basic", prop)
//
    if (sql == null) {
      LoggerFactory
        .getLogger(Constants.SPARK_APP_NAME_USER.getClass)
        .debug("哈哈~~，提交的表名参数有问题")
    } else {
      val finalSql = sql.replace("?", args(1))
      val df = sqlContext.sql(finalSql)

      if (args(0).split('.')(0).equals("qfbap_dm")) {
        // 获取配置信息
        val mysqlTableName = args(0).split('.')(1)
        val hiveTableName = args(0)
        val jdbcProp = getJdbcProp._1
        val jdbcUrl = getJdbcProp._2

        // 将数据写入mysql
        df.write.mode("append").jdbc(jdbcUrl, mysqlTableName, jdbcProp)
        // 将数据写入hive的dm层
        //df.write.mode(SaveMode.Append).insertInto(hiveTableName)
      }
    }
  }

  /**
    * 生成写入mysql的配置信息
    * @return
    */
  def getJdbcProp: (Properties, String) = {
    val prop = new Properties()
    prop.put("user", ConfigManager.getProperty("jdbc.user"))
    prop.put("password", ConfigManager.getProperty("jdbc.password"))
    prop.put("driver", ConfigManager.getProperty("jdbc.driver"))
    val jdbcUrl = ConfigManager.getProperty("jdbc.url")
    (prop, jdbcUrl)
  }

}
