package com.sdg.consumer.myutils


import java.text.DecimalFormat
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}

object HBaseUtil {
  /**
    * regionCode_call1_buildTime_call2_flag_duration
    *
    * @param regionCode 区号
    * @param caller     主叫号码
    * @param buildTime  建立时间
    * @param callee     被叫号码
    * @param flag       主动被动标识
    * @param duration   通话时长
    * @return
    */
  def genRowKey(regionCode: String, caller: String, buildTime: String, callee: String, flag: String, duration: String): String = {
    val sb = new StringBuilder
    sb.append(regionCode + "_")
      .append(caller + "_")
      .append(buildTime + "_")
      .append(caller + "_")
      .append(flag + "_")
      .append(duration)
    sb.toString()
  }


  /**
    * 获取区号
    *
    * @param call1
    * @param buildTime
    * @param regions
    * @return
    */
  def genRegionCode(call1: String, buildTime: String, regions: Integer): String = {
    //电话号码的长度
    val len: Int = call1.length
    //取出后4位号码
    val lastPhone: String = call1.substring(len - 4)
    //取出建立通过时间的年月2018-02-02
    val ym: String = buildTime.replaceAll("-", "")
      .replaceAll(":", "")
      .replaceAll(" ", "")
      .substring(0, 6)
    //离散操作1 ^  这个符号是异或运算  转成二进制 对应位置相同为 0   不同就为1
    val x: Integer = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym)
    //离散操作2
    val y: Int = x.hashCode
    //生成分区号
    val regionCode: Int = y % regions
    //格式化分区号
    val df = new DecimalFormat("00")

    df.format(regionCode)
  }


  /**
    * 预分区键
    *
    * @param regions
    * @return
    */
  def genSplitKeys(regions: Integer): Array[Array[Byte]] = {
    //定义一个存放分区键的数组
    val keys: Array[String] = new Array[String](regions)
    //目前推算，region个数不会超过2位数，所以region分区键格式化为两位数字所代表的字符串
    val df: DecimalFormat = new DecimalFormat("00")
    //对region个数遍历
    for (i <- 0 until regions) {
      //使用 | 拼接一下
      keys(i) = df.format(i) + "|"
    }
    //定义一个二维数组
    val splitKeys = new Array[Array[Byte]](regions)
    //比较器 BYTES_COMPARATOR :升序排序
    val treeSet: util.TreeSet[Array[Byte]] = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    for (i <- 0 until regions) {
      //把我们生成keys 放进去
      treeSet.add(Bytes.toBytes(keys(i)));
    }

    val splitKeysIterator: util.Iterator[Array[Byte]] = treeSet.iterator
    var index = 0
    while (splitKeysIterator.hasNext) {
      val b: Array[Byte] = splitKeysIterator.next
      println(b)

      splitKeys(index) = b
      index = index + 1
    }
    splitKeys
  }

  /**
    *
    * @param conf
    * @param tableName    表名
    * @param regions      分区个数
    * @param columnFamily 列簇（连个列簇）
    */
  def createTable(conf: Configuration, tableName: String, regions: Integer, columnFamily: String*) = {
    val connection: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = connection.getAdmin
    //if (isExistTable(conf, tableName)) return
    val htd = new HTableDescriptor(TableName.valueOf(tableName))
    for (cf <- columnFamily) {
      htd.addFamily(new HColumnDescriptor(cf))
    }
    //创建表的时候指定支持协处理器
    //htd.addCoprocessor("hbase.CalleeWriteObserver")
    //指定与分区指定分区的个数
    admin.createTable(htd, genSplitKeys(regions))
    admin.close()
    connection.close()
  }

  def main(args: Array[String]): Unit = {
    /*  val conf: Configuration = HbaseDao.conf
      val connection: Connection = ConnectionFactory.createConnection(conf)
      val admin: Admin = connection.getAdmin
      //if (isExistTable(conf, tableName)) return
      val htd = new HTableDescriptor(TableName.valueOf(" "))
      //增加协处理器
      //htd.addCoprocessor("hbase.CalleeWriteObserver")

      //指定与分区指定分区的个数
      admin.createTable(htd, genSplitKeys(6))
      admin.close()
      connection.close()*/
    val str: String = genRegionCode("13526949099", "2018-02-02", 6)
    println(str)
  }

  /**
    * 初始化命名空间
    *
    * @param conf
    * @param namespace
    */
  def initNamespace(conf: Configuration, namespace: String) = {
    val connection: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = connection.getAdmin
    val nd: NamespaceDescriptor = NamespaceDescriptor.create(namespace).addConfiguration("CREATE_TIME", String.valueOf(System.currentTimeMillis)).addConfiguration("AUTHOR", "JinJi").build
    admin.createNamespace(nd)
    admin.close()
    connection.close()
  }

  /**
    * 判断表是否存在
    *
    * @param conf
    * @param tableName
    */
  def isExistTable(conf: Configuration, tableName: String): Boolean = {
    val connection: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = connection.getAdmin
    val result: Boolean = admin.tableExists(TableName.valueOf(tableName))
    admin.close()
    connection.close()
    result
  }


}

