package com.sdg.consumer.myutils

import java.io.InputStream
import java.util.Properties

/**
  * 读取配置文件信息
  */
object PropertiesUtil {
  val is: InputStream = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties")
  var properties = new Properties
  properties.load(is)

  //根据key 取出来对应的值
  def getProperty(key: String): String = {
    val str: String = properties.getProperty(key)
    str
  }

}

