package com.sdg.mr.mapper;

import com.sdg.mr.kv.key.ComDimension;
import com.sdg.mr.kv.key.ContactDimension;
import com.sdg.mr.kv.key.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 计算通话总时长
 * 1.TableMapper 是一个继承Mapper 的抽象类,但是mapper有四个参数泛型,为何tableMapper只有两个
 * 我们通过源码可以看出,tableMapper的keyIn ,valueIn 分别设置了 ImmutabelBytesWriteable和Result类型
 * 所以只需要实现KEYOUT，VALUEOUT即可
 * 2.这里的tableMapper 类是完全为了从hbsae中读取数据而设置的,也就是这个tableMappper是专门为hbsae 定义抽象了
 */
public class CountDurationMapper extends TableMapper<ComDimension, Text> {
    //混合维度 key
    private ComDimension comDimension = new ComDimension();
    // value
    private Text durationText = new Text();

    private Map<String, String> phoneNameMap;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        phoneNameMap = new HashMap<String, String>();
        phoneNameMap.put("17078388295", "李雁");
        phoneNameMap.put("13980337439", "卫艺");
        phoneNameMap.put("14575535933", "仰莉");
        phoneNameMap.put("19902496992", "陶欣悦");
        phoneNameMap.put("18549641558", "施梅梅");
        phoneNameMap.put("17005930322", "金虹霖");
        phoneNameMap.put("18468618874", "魏明艳");
        phoneNameMap.put("18576581848", "华贞");
        phoneNameMap.put("15978226424", "华啟倩");
        phoneNameMap.put("15542823911", "仲采绿");
        phoneNameMap.put("17526304161", "卫丹");
        phoneNameMap.put("15422018558", "戚丽红");
        phoneNameMap.put("17269452013", "何翠柔");
        phoneNameMap.put("17764278604", "钱溶艳");
        phoneNameMap.put("15711910344", "钱琳");
        phoneNameMap.put("15714728273", "缪静欣");
        phoneNameMap.put("16061028454", "焦秋菊");
        phoneNameMap.put("16264433631", "吕访琴");
        phoneNameMap.put("17601615878", "沈丹");
        phoneNameMap.put("15897468949", "褚美丽");
    }

    /**
     * @param key
     * @param value
     * @param context 全局对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //得到rowkeu 05_19902496992_20170312154840_15542823911_1_1288
        String rowKey = Bytes.toString(key.get());
        //切分数据
        String[] splits = rowKey.split("_");
        //判断是否是主叫      1：主叫      0：被叫   把被叫的给过滤掉
        if (splits[4].equals("0"))  return;

        //以下数据全部是主叫数据，但是也包含了被叫电话的数据
        //主叫号码
        String caller = splits[1];
        //被叫号码
        String callee = splits[3];
        //通话建立时间
        String buildTime = splits[2];
        //通话时长
        String duration = splits[5];
        durationText.set(duration);

        //年
        String year = buildTime.substring(0, 4);
        //月
        String month = buildTime.substring(4, 6);
        //日
        String day = buildTime.substring(6, 8);

        //组装ComDimension
        //组装DateDimension
        ////05_19902496992_20170312154840_15542823911_1_1288
        DateDimension yearDimension = new DateDimension(year, "-1", "-1");
        DateDimension monthDimension = new DateDimension(year, month, "-1");
        DateDimension dayDimension = new DateDimension(year, month, day);

        //组装ContactDimension (电话号码，姓名)
        ContactDimension callerContactDimension = new ContactDimension(caller, phoneNameMap.get(caller));

        //开始聚合主叫数据
        comDimension.setContactDimension(callerContactDimension);
        //年
        comDimension.setDateDimension(yearDimension);
        //自定义的key  value
        context.write(comDimension, durationText);
        //月
        comDimension.setDateDimension(monthDimension);
        context.write(comDimension, durationText);
        //日
        comDimension.setDateDimension(dayDimension);
        context.write(comDimension, durationText);

        //开始聚合被叫数据
        ContactDimension calleeContactDimension = new ContactDimension(callee, phoneNameMap.get(callee));
        comDimension.setContactDimension(calleeContactDimension);
        //年
        comDimension.setDateDimension(yearDimension);
        context.write(comDimension, durationText);
        //月
        comDimension.setDateDimension(monthDimension);
        context.write(comDimension, durationText);
        //日
        comDimension.setDateDimension(dayDimension);
        context.write(comDimension, durationText);

    }


}