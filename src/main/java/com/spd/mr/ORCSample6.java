package com.spd.mr;

import com.spd.util.HadoopFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

public class ORCSample6 {

    public static class ORCMapper6 extends Mapper<NullWritable, OrcStruct, Text, Text> {
        private Text oKey=new Text();
        private Text oValue=new Text();
        public void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            StringBuffer bf = new StringBuffer();
            if(value.getNumFields()>=3){
                Text valAcount=(Text)value.getFieldValue(0);
                Text valDomain=(Text)value.getFieldValue(1);
                Text valPost=(Text)value.getFieldValue(2);
                bf.append(valAcount.toString()).append("|").append(valDomain.toString()).append("|").append(valPost.toString());
            }
            if (bf.length() > 0) {
                oValue.set(bf.toString());
            }else{
                oValue.set("");
            }
            oKey.set("");
            System.out.println(bf.toString());
            context.write(oKey,oValue);
        }
    }


    public static class ORCReducer6 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                String strVal=value.toString();
                Text okey=new Text();
                okey.set(strVal);
                context.write(null,okey);
            }
        }
    }


    public static void main(String[] args) {

        //打印输入参数
        for (int i = 0; i < args.length ;i++){
            System.out.println("arr["+i+"] >> "+args[i]);
        }
        try{
            Configuration conf  =new Configuration();

            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            for (int i = 0; i < otherArgs.length ;i++){
                System.out.println("arr["+i+"] >> "+otherArgs[i]);
            }

            //分隔符
             String split =otherArgs[0].trim();

            //输入路径
            String in_path = otherArgs[1].trim();

            //输出路径
             String out_path = otherArgs[2].trim();

            //匹配host的路径
            String host_path = otherArgs[3].trim();
            conf.set("host_path",host_path);

            boolean inputFile = HadoopFileUtil.isFileExist ( in_path,conf );
            boolean outputFile = HadoopFileUtil.isFileExist ( out_path );
            if ( !inputFile ) {
                System.out.println ( "Input File Not Exist !" );
                return;
            }
            if ( outputFile ) {
                System.out.println ( "Output File Already Exist !" );
                return;
            }

            Job job = Job.getInstance(conf,"ORCSample6");
            job.setJarByClass(ORCSample6.class);
            job.setMapperClass(ORCMapper6.class);
            job.setReducerClass(ORCReducer6.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(in_path));
            FileOutputFormat.setOutputPath(job, new Path(out_path));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch (Exception e){
            System.out.println(e.toString());
            System.exit(1);
        }
    }



}
