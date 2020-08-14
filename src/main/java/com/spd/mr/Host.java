package com.spd.mr;

import com.spd.util.DpiPathFilter;
import com.spd.util.HadoopFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.mapreduce.OrcInputFormat;

public class Host {

    public static void main(String[] args) {

        //打印输入参数
        for (int i = 0; i < args.length ;i++){
            System.out.println("arr["+i+"] >> "+args[i]);
        }

        try{
            //分隔符
            String split =args[0].trim();

            //输入路径
            String in_path = args[1].trim();

            //输出路径
            String out_path = args[2].trim();

            //匹配host的路径
            String host_path = args[3].trim();

            Configuration conf  =new Configuration();
            conf.set ( "mapreduce.map.failures.maxpercent", "90" );
            conf.set ( "mapreduce.reduce.failures.maxpercent", "90" );
           // conf.set ( "mapred.job.queue.name", queue );
            conf.set("split", split);
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

            Job job = Job.getInstance(conf,"mr_hostMatch");
            job.setJarByClass(Host.class);
            job.setMapperClass(HostMapper.class);
            job.setReducerClass(HostReducer.class);
            job.setNumReduceTasks(20);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(HostBean.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(OrcInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

           // FileInputFormat.setInputPathFilter(job, DpiPathFilter.class);
            //FileInputFormat.addInputPath(job, new Path(in_path));
            OrcInputFormat.addInputPath(job, new Path(in_path));

            FileOutputFormat.setOutputPath(job, new Path(out_path));
            // 多子job的类中，可以保证各个子job串行执行
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e){
            System.out.println(e.toString());
            System.exit(1);
        }
    }
}
