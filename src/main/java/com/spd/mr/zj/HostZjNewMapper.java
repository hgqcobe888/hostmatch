package com.spd.mr.zj;
import com.spd.util.HadoopFileUtil;
import com.spd.util.MatcherUtilNew;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HostZjNewMapper extends Mapper<LongWritable, Text, Text, HostBeanZj> {
    private HostBeanZj bean = new HostBeanZj();
    private final static MatcherUtilNew m = new MatcherUtilNew();
    private final static String EMPTY = "";

    public static HashMap<String,String> COMPARES = new HashMap<>();
    public static String SPLIT = "0";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        SPLIT = context.getConfiguration().get("split");
        if("0".equals(SPLIT)){
            SPLIT = "|";
        }else{
            SPLIT = "\005";
        }

        //匹配host
        String path = context.getConfiguration().get("host_path");
        COMPARES = HadoopFileUtil.readTextString(path,COMPARES);//['m.95303.com','C0']
        for (String string : COMPARES.keySet()){
            m.addPattern(string.toUpperCase(),COMPARES.get(string));
        }
        context.getCounter("DEBUG", "SPLIT "+SPLIT + " ").increment(1);
        context.getCounter("DEBUG", "COMPARE_FILE_PATH "+path + " ").increment(1);
        context.getCounter("DEBUG", "COMPARE_SIZE " + COMPARES.size() +" ").increment(1);

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

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {

            String[] arr = StringUtils.splitPreserveAllTokens(value.toString(), "\\|");
            if(arr.length>=8){
                String mobile = arr[1];
                if(StringUtils.isBlank(mobile)){
                    context.getCounter("DEBUG", "mobile |" + mobile).increment(1);
                    return;
                }
                String destination = (arr[8] == null ? EMPTY : arr[8]).toUpperCase(); //http://sngmta.qq.com:80/mstat/report//?index=1530457578
                Map<String,String> results = new HashMap<>();
                if(StringUtils.isNotBlank(destination)){
                    context.getCounter("DEBUG", "destination |").increment(1);
                    int index = destination.indexOf("HTTP://");
                    if(index!=-1){
                        destination = destination.substring(index+7);
                    }
                    results = m.matchs_new(destination);

                }
                if(results==null || results.size()==0){
                    context.getCounter("DEBUG", "Result bank |" +results.size()).increment(1);
                    return;
                }

                if(results!=null && results.size() >0){
                    for (String r:results.keySet()){
                        bean.setImei(mobile);
                        bean.setUri(r);
                        bean.setCount(1);
                        context.write(new Text(mobile+"|"+results.get(r)),bean); //r.data=>C0
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
