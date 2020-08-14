package com.spd.mr.zj;

import com.spd.util.HadoopFileUtil;
import com.spd.util.MatcherUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class HostZjMapper extends Mapper<LongWritable, Text, Text, HostBeanZj> {

    private HostBeanZj bean = new HostBeanZj();
    private final static MatcherUtil m = new MatcherUtil();
    private final static String EMPTY = "";

    public static HashMap<String,String> COMPARES = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //匹配host
        String path = context.getConfiguration().get("host_path");
        COMPARES = HadoopFileUtil.readTextString(path,COMPARES);//['m.95303.com','C0']
        for (String string : COMPARES.keySet()){
            m.addPattern(string.toUpperCase(),COMPARES.get(string));
        }

        context.getCounter("DEBUG", "COMPARE_FILE_PATH "+path + " ").increment(1);
        context.getCounter("DEBUG", "COMPARE_SIZE " + COMPARES.size() +" ").increment(1);
        System.out.println("COMPARE_SIZE "+ COMPARES.size());

        if (COMPARES.size() == 0){
            COMPARES.put("V6-DY.IXIGUA.COM","C0");
            COMPARES.put("WX.QLOGO.CN","C1");
            COMPARES.put("LOG.TBS.QQ.COM","C2");
            COMPARES.put("MLEXTSHORT.WEIXIN.QQ.COM","C3");
            COMPARES.put("VV.VIDEO.QQ.COM","C4");
            COMPARES.put("P9.PSTATP.COM","C5");
            COMPARES.put("SZSHORT.WEIXIN.QQ.COM","C6");
            for (String string : COMPARES.keySet()){
                m.addPattern(string.toUpperCase(),COMPARES.get(string));
            }
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] arr = StringUtils.splitPreserveAllTokens(value.toString(), "\\|");

        if(arr.length>=8){
            String mobile = arr[1];
            if(StringUtils.isBlank(mobile)){
                context.getCounter("DEBUG", "mobile |" + mobile).increment(1);
                return;
            }

            String host = (arr[7] == null ? EMPTY : arr[7]).toUpperCase();   //sngmta.qq.com:80
            String destination = (arr[8] == null ? EMPTY : arr[8]).toUpperCase(); //http://sngmta.qq.com:80/mstat/report//?index=1530457578


            MatcherUtil.MatchResult[] results = {};

            if(results.length == 0 && StringUtils.isNotBlank(host)){
                context.getCounter("DEBUG", "host |").increment(1);
                results = m.matchs(host);
            }

            if(results.length == 0 && StringUtils.isNotBlank(destination)){
                context.getCounter("DEBUG", "destination |").increment(1);

                int index = destination.indexOf("HTTP://");
                if(index!=-1){
                    destination = destination.substring(index+7);
                }

                index = destination.indexOf("?");
                if (index != -1){
                    destination = destination.substring(0, index);
                }
                results = m.matchs(destination);
            }

            if(results.length ==0){
                context.getCounter("DEBUG", "Result bank |" +results.length).increment(1);
                return;
            }

            if(results!=null && results.length >0){
                for (MatcherUtil.MatchResult r : results) {
                    bean.setImei(mobile);
                    bean.setUri(r.pattern);
                    bean.setCount(1);
                    context.write(new Text(mobile+"|"+r.data),bean); //r.data=>C0
                }
            }
        }
    }

}
