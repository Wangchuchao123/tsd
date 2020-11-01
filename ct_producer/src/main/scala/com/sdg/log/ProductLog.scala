package com.sdg.log

import java.io.{FileOutputStream, OutputStreamWriter}
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

object ProductLog {
  private val startTime = "2017-01-01"
  private val endTime = "2017-12-31"

  private val phoneList = ListBuffer[String]()
  private val phoneNameMap: mutable.Map[String, String] = mutable.Map[String, String]()

  def main(args: Array[String]): Unit = {
  //路径参数可以运行的时候指定
      if (args == null || args.length <= 0) {
        System.out.println("no arguments")
        //结束程序
        return
      }
    //测试的时候直接写死
    //val logPath = "D:\\calllog.csv";
    //初始化数据
    ProductLog.initPhone()
    //数据写入到指定的文件
    //ProductLog.writeLog(logPath)
     ProductLog.writeLog(args(0))
  }

  /**
    * 初始化电话信息
    */
  def initPhone(): Unit = {
    phoneList.append("17078388295")
    phoneList.append("13980337439")
    phoneList.append("14575535933")
    phoneList.append("19902496992")
    phoneList.append("18549641558")
    phoneList.append("17526304161")
    phoneList.append("15422018558")
    phoneList.append("17269452013")
    phoneList.append("17764278604")
    phoneList.append("15711910344")
    phoneList.append("15714728273")
    phoneList.append("16061028454")
    phoneList.append("16264433631")
    phoneList.append("17601615878")
    phoneList.append("15897468949")

    phoneNameMap.put("17078388295", "李雁")
    phoneNameMap.put("13980337439", "卫艺")
    phoneNameMap.put("14575535933", "仰莉")
    phoneNameMap.put("19902496992", "陶欣悦")
    phoneNameMap.put("18549641558", "施梅梅")
    phoneNameMap.put("17005930322", "金虹霖")
    phoneNameMap.put("18468618874", "魏明艳")
    phoneNameMap.put("18576581848", "华贞")
    phoneNameMap.put("15978226424", "华啟倩")
    phoneNameMap.put("15542823911", "仲采绿")
    phoneNameMap.put("17526304161", "卫丹")
    phoneNameMap.put("15422018558", "戚丽红")
    phoneNameMap.put("17269452013", "何翠柔")
    phoneNameMap.put("17764278604", "钱溶艳")
    phoneNameMap.put("15711910344", "钱琳")
    phoneNameMap.put("15714728273", "缪静欣")
    phoneNameMap.put("16061028454", "焦秋菊")
    phoneNameMap.put("16264433631", "吕访琴")
    phoneNameMap.put("17601615878", "沈丹")
    phoneNameMap.put("15897468949", "褚美丽")
  }

  /**
    * 根据传入的时间区间，在此范围内随机通话建立的时间
    *
    * @param startTime
    * @param endTime
    * @return
    */
  def randomBuildTime(startTime: String, endTime: String): String = {
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
    val startDate: Date = sdf1.parse(startTime)
    val endDate: Date = sdf1.parse(endTime)
    if (endDate.getTime <= startDate.getTime) return null

    val randomTS: Long = startDate.getTime + ((endDate.getTime - startDate.getTime) * Math.random).toLong
    val resultDate = new Date(randomTS)
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val resultTimeString: String = sdf2.format(resultDate)
    resultTimeString
  }

  /**
    * 产生日志数据
    *
    * @return
    */
  def product(): String = {
    //主叫号码
    var caller: String = null
    //主叫号码对应的名字
    var callerName: String = null
    //被叫号码
    var callee: String = null
    //被叫号码对应的名字
    var calleeName: String = null
    //取得主叫电话号码
    val callerIndex: Int = (Math.random * phoneList.size).toInt
    //随即取出一个号码
    caller = phoneList(callerIndex)
    //取出根据号码对应的名字
    callerName = phoneNameMap.get(caller).get
    val breaks = new Breaks
    breaks.breakable {
      while (true) {
        //取得被叫电话号码
        val calleeIndex: Int = (Math.random * phoneList.size).toInt
        //从list中选出其中一个号码，作为被叫号码
        callee = phoneList(calleeIndex)
        //被叫对应的名称
        calleeName = phoneNameMap.get(callee).get
        //如果两个电话号码是一样的，就结束本次循环
        if (!(caller == callee)) {
          breaks.break()
        }

      }
    }
    //聊天建立的时间
    val buildTime: String = randomBuildTime(startTime, endTime)
    //0000
    val df = new DecimalFormat("0000")
    val duration: String = df.format((30 * 60 * Math.random).toInt)
    val sb = new StringBuilder
    //主叫号码  被叫号码  通话时间  通话时长   通话标识   注:主叫姓名 和被叫姓名也是可以加上
    sb.append(caller + ",").append(callerName + ",").append(callee + ",").append(calleeName + ",").append(buildTime + ",").append(duration + ",").append("1");
    sb.toString
  }

  /**
    *
    * @param filePath
    */
  def writeLog(filePath: String): Unit = {
    //将数据写入到文件中
    val osw = new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8")
    val breaks = new Breaks
    while (true) {
      //休眠时间
      Thread.sleep(5000)
      //生成数据
      val log: String = product()
      //打印数据
      println("日志数据： " + log)
      //把数据写入到指定的路径
      osw.write(log + "\n")
      //一定要手动flush才可以确保每条数据都写入到文件一次
      osw.flush()

    }
  }
}
