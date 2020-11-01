package com.sdg.mr.runner;

import com.sdg.mr.kv.key.ComDimension;
import com.sdg.mr.kv.value.CountDurationValue;
import com.sdg.mr.mapper.CountDurationMapper;
import com.sdg.mr.reducer.CountDurationReducer;
import com.sdg.mr.outputformat.MysqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class CountDurationRunner implements Tool {
    private Configuration conf = null;

    @Override //在Configurable 接口中
    public void setConf(Configuration conf) {
        //hbase 的配置
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override //在Configurable 接口中
    public Configuration getConf() {
        return this.conf;
    }

    @Override //在 Tool 接口中
    public int run(String[] args) throws Exception {
        //得到conf
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        System.out.println("123--------");
        //设置运行的类
        job.setJarByClass(CountDurationRunner.class);
        //设置map阶段 从hbase中读取数据
        initHBaseInputConfig(job);
        //组装Reducer Outputformat
        initReducerOutputConfig(job);
        //运行等待完成    完成0  没有完成返回 1
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 初始化reducer阶段的配置
     *
     * @param job job 对象
     */
    private void initReducerOutputConfig(Job job) {
        //设置reducer阶段
        job.setReducerClass(CountDurationReducer.class);
        //设置输出的key
        job.setOutputKeyClass(ComDimension.class);
        //设置输出的value
        job.setOutputValueClass(CountDurationValue.class);
        //自定义输出（把结果数据写入到mysql）
        job.setOutputFormatClass(MysqlOutputFormat.class);
    }

    /**
     * map阶段 从hbase中读取数据
     * @param job
     */

    private void initHBaseInputConfig(Job job) {
        Connection connection = null;
        Admin admin = null;
        try {
            //命名空间:表名
            String tableName = "ns_ct:calllog";
            //获取连接
            connection = ConnectionFactory.createConnection(job.getConfiguration());
            //得到admin对象
            admin = connection.getAdmin();
            //如果表在hbase中不存在
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                throw new RuntimeException("无法找到目标表.");
            }
            //创建与一个scan对象
            Scan scan = new Scan();
            //设置scan的属性
            //scan.setStartRow(Bytes.toBytes("row-1"));
            //scan.setStopRow(Bytes.toBytes("row-9"));
            // scan.addColumn(Bytes.toBytes(""),Bytes.toBytes(""));
            //scan.addFamily(Bytes.toBytes(""));
            //可以优化
            //初始化Mapper
            //hbase-server中提供 ，使用mr 从文件中读取数据写入到hbase的时候用到
            //TableMapReduceUtil，只是简单的配置之后，工具类会帮我们生成写入到hbase中的工具类，
            //工具类中封装了许多MapReduce写入到HBase的操作，无需我们自己再去设置
            //从hbase中读取读取数据

            TableMapReduceUtil.initTableMapperJob(
                    tableName, //表的名称
                    scan, //scan对象
                    CountDurationMapper.class,  //map的class
                    ComDimension.class, //输出的key
                    Text.class,  //输出的value
                    job,//job 对象
                    true); //通过分布式缓存(tmpjars)上传作业类的HBase jar。


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 主方法入口
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            System.out.println("开始运行-----------------");
            //执行job (hadoop-common)
            //返回int 类型状态 0:运行正常结束  非0：运行不正常结束
            //hadoop-commom 提供的一个工具类  为什么要适应ToolRunner ?
            int status = ToolRunner.run(new CountDurationRunner(), args);
            System.out.println(status);
            System.out.println("开始结束-----------------");
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}