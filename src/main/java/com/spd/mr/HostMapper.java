package com.spd.mr;

import com.spd.util.HadoopFileUtil;
import com.spd.util.MatcherUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.HashMap;

public class HostMapper extends Mapper<NullWritable, OrcStruct,Text,HostBean> {
    private HostBean bean = new HostBean();
    private final static MatcherUtil m = new MatcherUtil();
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
    protected void map(NullWritable  key, OrcStruct  value, Context context) throws IOException, InterruptedException {
        try {

            //String[] arr = StringUtils.splitPreserveAllTokens(value.toString(), SPLIT);
            String mobile = value.getFieldValue(1).toString();
            System.out.println(mobile);
            if(StringUtils.isBlank(mobile)){
                context.getCounter("DEBUG", "mobile |" + mobile).increment(1);
                return;
            }

            String destination = (value.getFieldValue(8).toString() == null ? EMPTY : value.getFieldValue(8).toString()).toUpperCase(); //http://sngmta.qq.com:80/mstat/report//?index=1530457578
            String host = (value.getFieldValue(7).toString() == null ? EMPTY : value.getFieldValue(7).toString()).toUpperCase();   //sngmta.qq.com:80

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
                    //destination = destination.substring(0, index);
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
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
