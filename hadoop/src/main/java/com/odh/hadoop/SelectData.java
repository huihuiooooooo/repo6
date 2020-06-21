package com.odh.hadoop;

import com.odh.entity.YearMaxTAndMinT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SelectData {
    public static class SelectDataMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split(",");
            //筛选1、2月份的数据
            if(strings[1].contains("2016-01") || strings[1].contains("2016-02")){
                context.write(new Text(strings[0]), new Text(strings[1]));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("error <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "select data");
        job.setJarByClass(SelectData.class); //起点
        job.setMapperClass(SelectDataMapper.class); //自定义Mapper
        job.setOutputKeyClass(Text.class); //k4
        job.setOutputValueClass(Text.class); //v4
        job.setInputFormatClass(TextInputFormat.class);//设置输入格式
        job.setOutputFormatClass(SequenceFileOutputFormat.class);//设置输出格式
        job.setNumReduceTasks(0);//设置Reducer任务数为0
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //设置输入文件的路径

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); //设置输出文件的路径


        System.exit(job.waitForCompletion(true) ? 0 : 1);// 执行
    }
}
