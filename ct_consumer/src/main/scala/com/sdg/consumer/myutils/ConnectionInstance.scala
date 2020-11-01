package com.sdg.consumer.myutils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
  * 获取连接的
  */
object ConnectionInstance {
  private var conn: Connection = null

  def getConnection(conf: Configuration): Connection = {

    if (conn == null || conn.isClosed) {
      conn = ConnectionFactory.createConnection(conf);
    }
    conn
  }

}

