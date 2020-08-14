package com.spd.mr.zj.fillter;

import com.spd.mr.zj.HostBeanZj;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HostZjFillterMapper extends Mapper<LongWritable, Text, Text, HostZjFillterBean>{

    private HostZjFillterBean bean = new HostZjFillterBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString ();
            String[] arr = StringUtils.splitPreserveAllTokens(line, "\\|");
            String mobile="";
            if(null != arr) {
                if(arr[0].startsWith("#")){
                    //当日新增
                    mobile=arr[1];
                    bean.setImei(arr[1]);
                    bean.setUri(arr[2]);
                    bean.setCount( Long.parseLong(arr[3]) );
                    bean.setDatatype("0");
                }else{
                    mobile=arr[0];
                    bean.setImei(arr[0]);
                    bean.setUri(arr[1]);
                    bean.setCount( Long.parseLong(arr[2]) );
                    bean.setDatatype("1");
                }

            }
            context.write ( new Text(mobile+"|"+bean.getUri()),bean);
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
    }
}
