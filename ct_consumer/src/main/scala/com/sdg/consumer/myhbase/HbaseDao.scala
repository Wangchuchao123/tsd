package com.sdg.consumer.myhbase


import java.text.SimpleDateFormat
import java.util

import com.sdg.consumer.myutils.{ConnectionInstance, HBaseUtil, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

object HbaseDao {

  private val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val sdf2 = new SimpleDateFormat("yyyyMMddHHmmss")
  private val cacheList = new util.ArrayList[Put]
  var conf: Configuration = HBaseConfiguration.create
  private var regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"))
  private var namespace: String = PropertiesUtil.getProperty("hbase.calllog.namespace")
  private var tableName: String = PropertiesUtil.getProperty("hbase.calllog.tablename")
  var table: HTable = null
  //首先创建命名空间  然后再创建表
  if (!HBaseUtil.isExistTable(conf, tableName)) {
    //这里做一个判断就好
    HBaseUtil.initNamespace(conf, namespace)
    HBaseUtil.createTable(conf, tableName, regions, "f1", "f2")
  }


  /**
    * 把数据写入到hbase
    * ori数据样式： 18576581848,17269452013,2017-08-14 13:38:31,1761
    * rowkey样式：01_18576581848_20170814133831_17269452013_1_1761
    * HBase表的列：call1  call2   build_time   build_time_ts   flag   duration
    *
    * @param ori
    */
  def put(ori: String): Unit = {

    if (cacheList.size == 0) {
      val connection = ConnectionInstance.getConnection(conf)
      table = connection.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
      table.setAutoFlushTo(false)
      //
      table.setWriteBufferSize(2 * 1024 * 1024)
    }
    //对传输过来的字符串用逗号进行分割
    val splitOri: Array[String] = ori.split(",")
    val caller: String = splitOri(0)
    val callee: String = splitOri(2)
    val buildTime: String = splitOri(4)
    val duration: String = splitOri(5)
    //获取region编码
    val regionCode: String = HBaseUtil.genRegionCode(caller, buildTime, regions)
    //建立通话时间
    val buildTimeReplace: String = sdf2.format(sdf1.parse(buildTime))
    val buildTimeTs: String = String.valueOf(sdf1.parse(buildTime).getTime)
    //生成rowkey
    val rowkey: String = HBaseUtil.genRowKey(regionCode, caller, buildTimeReplace, callee, "1", duration)
    //向表中插入该条数据
    val put: Put = new Put(Bytes.toBytes(rowkey))
    //主叫号码
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call1"), Bytes.toBytes(caller))
    //被叫号码
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call2"), Bytes.toBytes(callee))
    //通话日期
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime))
    //通话时间
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTimeTs))
    //通过标识
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"))
    //通话时长
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration))
    cacheList.add(put)
    if (cacheList.size >= 10) {
      // val value: List[Put] =  util.List[Put]
      //这个类型必须是java的List,scala 的类型是不支持的
      table.put(cacheList)
      table.flushCommits()
      cacheList.clear()
    }
    //1.定时接受数据
    //2.for循环之外在提交一次
  }

}

