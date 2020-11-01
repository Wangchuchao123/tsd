package com.sdg.consumer.mykafka

package kafka
//命名包名的时候不要冲突
import com.sdg.consumer.myutils.PropertiesUtil
import com.sdg.consumer.myhbase.HbaseDao

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import java.util
import scala.collection.JavaConversions._

/**
  * 把数据写入到hbase 中
  */
object HbaseConsumer {

  def main(args: Array[String]): Unit = {
    //testHbase()
    //创建kafka消费者的对象
    val kafkaConsumer = new KafkaConsumer[String, String](PropertiesUtil.properties)
    //订阅指定的topic  用于数据的消费
    kafkaConsumer.subscribe(util.Arrays.asList(PropertiesUtil.getProperty("kafka.topics")))
    println("等待消费数据--------------")
    while (true) {
      //每0.1S 从指定topic中消费数据
      val records: ConsumerRecords[String, String] = kafkaConsumer.poll(100)
      //这个是scala和java集合类型之间的转换
      for (cr <- records) {
        //得到每条数据的value
        val str: String = cr.value()
        println(str)
        //把数据写入到hbase中
        HbaseDao.put(str)
      }
    }

  }

  def testHbase(): Unit = {
    val str = "18576581848,17269452013,2017-08-14 13:38:31,1761"
    HbaseDao.put(str)
  }

}

