package com.spd.mr.zj.fillter;
import com.spd.util.DateUtil;
import com.spd.util.HadoopFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HostZjFillter {

    public static void main(String[] args) {

        //打印输入参数
        for (int i = 0; i < args.length ;i++){
            System.out.println("arr["+i+"] >> "+args[i]);
        }

        try{

            String in_path = args[0].trim();       //输入路径
            String out_path = args[1].trim();      //输出路径
            String fillter_path = args[2].trim();  //过滤数据逻辑
            String current_day = args[3].trim();  //当前日期
            String fillter_day = args[4].trim();   //过滤天数

            Configuration conf  =new Configuration();
            conf.set ( "mapreduce.map.failures.maxpercent", "90" );
            conf.set ( "mapreduce.reduce.failures.maxpercent", "90" );
            // conf.set ( "mapred.job.queue.name", queue );

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

            Job job = Job.getInstance(conf,"mr_hostMatch_fillter");
            job.setJarByClass(HostZjFillter.class);
            job.setMapperClass(HostZjFillterMapper.class);
            job.setReducerClass(HostZjFillterReducer.class);
            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(HostZjFillterBean.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //FileInputFormat.addInputPath(job, new Path(in_path));
            MultipleInputs.addInputPath(job, new Path(in_path), TextInputFormat.class, HostZjFillterMapper.class);

            //增加过滤路径
            if(Integer.parseInt(fillter_day)>0){
                for (int d=1;d<=Integer.parseInt(fillter_day);d++){
                    String tmp = fillter_path.replace("_FILTER_DATE_", DateUtil.getDayBefore(current_day,d));
                    System.out.println(tmp);
                    if(HadoopFileUtil.isFileExist ( tmp )){
                        System.out.println("目录："+tmp+",存在，加载数据......");
                        MultipleInputs.addInputPath(job, new Path(tmp), TextInputFormat.class, HostZjFillterMapper.class);
                    }
                }
            }
            FileOutputFormat.setOutputPath(job, new Path(out_path));
            // 多子job的类中，可以保证各个子job串行执行
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (Exception e){
            System.out.println(e.toString());
            System.exit(1);
        }
    }
}
