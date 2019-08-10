package utils

import conf.ConfigManager
import constant.Constants
import org.apache.spark.SparkConf

object SparkUtils {
  /**
    * 根据当前的配置设置是否为local模式
    * @param conf
    */
  def setMaster(conf: SparkConf): Unit = {
    val local = ConfigManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) conf.setMaster("local[*]")
  }


}
