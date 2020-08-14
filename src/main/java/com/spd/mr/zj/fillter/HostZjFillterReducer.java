package com.spd.mr.zj.fillter;

import com.spd.mr.zj.HostBeanZj;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HostZjFillterReducer extends Reducer<Text, HostZjFillterBean, Text, Text> {

    public void reduce(Text key, Iterable<HostZjFillterBean> lists, Context context) throws IOException, InterruptedException {

        boolean isolddata = false;
        try{
            long count = 0;
            for (HostZjFillterBean bean:lists){
                if(bean.getDatatype().equals("1")){
                    isolddata=true;
                }
                count = count+bean.getCount();
            }

            if(isolddata){
                return;
            }
            context.write(null,new Text(key+"|"+count));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
