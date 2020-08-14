package com.spd.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HostReducer extends Reducer<Text,HostBean,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<HostBean> lists, Context context) throws IOException, InterruptedException {
        try{
            long count = 0;
            for (HostBean bean:lists){
                count = count+bean.getCount();
            }
            context.write(null,new Text(key+"|"+count));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
