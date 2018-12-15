package cn.dmp.utils

import java.util.Properties

/**
  * 配置管理组件
  *
  * @author jacks
  */
object ConfUtils {
  private val inputStream= this.getClass.getResourceAsStream("/application.properties")
  private val prop = new Properties
  prop.load(inputStream)

  def getString(key: String) = prop.getProperty(key)

  def getInt(key: String) = getString(key).toInt

  def getBoolean(key: String) = getString(key).toBoolean

  def getLong(key: String) = getString(key).toLong
}
