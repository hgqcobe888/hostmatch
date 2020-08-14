package com.spd.mr.zj;

import com.spd.mr.HostBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HostZjReducer extends Reducer<Text, HostBeanZj, Text, Text> {

    public void reduce(Text key, Iterable<HostBeanZj> lists, Context context) throws IOException, InterruptedException {
        try{
            long count = 0;
            for (HostBeanZj bean:lists){
                count = count+bean.getCount();
            }
            context.write(null,new Text("#|"+key+"|"+count));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}

