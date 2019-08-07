package conf

import java.util.Properties

/**
  * SQL管理组件
  * 提供key对应的SQL语句
  */
object ConfigManager {
  private val prop = new Properties()

  try {
    // 通过调用类加载器（ClassLoader）的getResourceAsStream方法获取指定文件的输入流
    val in_basic = ConfigManager.getClass.getClassLoader.getResourceAsStream("basic.properties")
    val in_ods_dwd = ConfigManager.getClass.getClassLoader.getResourceAsStream("ods_dwd.properties")
    val in_dwd_dws = ConfigManager.getClass.getClassLoader.getResourceAsStream("dwd_dws.properties")
    val in_dwd_dm = ConfigManager.getClass.getClassLoader.getResourceAsStream("dwd_dm.properties")

    prop.load(in_basic)
    prop.load(in_ods_dwd)
    prop.load(in_dwd_dws)
    prop.load(in_dwd_dm)

  } catch {
    case e: Exception => e.printStackTrace()
  }

  /**
    * 获取指定key对应的value
    *
    * @param key
    * @return
    */
  def getProperty(key: String): String = {
    prop.getProperty(key)
  }

  /**
    * 获取布尔类型的配置项
    *
    * @param key
    * @return
    */
  def getBoolean(key: String): Boolean = {
    val value: java.lang.String = getProperty(key)
    try {
//      Boolean.unbox(value) // 抛异常
      return java.lang.Boolean.valueOf(value)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    false
  }

}
