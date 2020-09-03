package com.spd.mr.zj;

import com.spd.mr.HostBean;
import com.spd.mr.HostMapper;
import com.spd.mr.HostReducer;
import com.spd.util.HadoopFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.mapreduce.OrcInputFormat;

public class HostZj {

    public static void main(String[] args) {

        //打印输入参数
        for (int i = 0; i < args.length ;i++){
            System.out.println("arr["+i+"] >> "+args[i]);
        }

        try{
            //输入路径
            String in_path = args[0].trim();

            //输出路径
            String out_path = args[1].trim();

            //匹配host的路径
            String host_path = args[2].trim();

            //输出控制  0:输出手机和imei,1:只输出手机  2:只输出imei
            String outcontrol= args[3].trim();

            Configuration conf  =new Configuration();
            conf.set ( "mapreduce.map.failures.maxpercent", "90" );
            conf.set ( "mapreduce.reduce.failures.maxpercent", "90" );
           // conf.set ( "mapred.job.queue.name", queue );

            conf.set("host_path",host_path);
            conf.set("outcontrol",outcontrol);
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

            Job job = Job.getInstance(conf,"mr_hostMatch_zj");
            job.setJarByClass(HostZj.class);
            job.setMapperClass(HostZjNewMapper.class);
            job.setReducerClass(HostZjReducer.class);
            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(HostBeanZj.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

           //job.setInputFormatClass(OrcInputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(in_path));
            FileOutputFormat.setOutputPath(job, new Path(out_path));
            // 多子job的类中，可以保证各个子job串行执行
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e){
            System.out.println(e.toString());
            System.exit(1);
        }
    }
}
