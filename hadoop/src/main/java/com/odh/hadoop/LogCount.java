package com.odh.hadoop;

import com.odh.entity.MemberLogTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;

/**
 * @// TODO: 2020/6/16 0016  优化日志文件统计程序
 * @流程  map输出<k2,v2>后，会经过自定义Partitioner对数据进行分区，一确保1、2月份数据分开
 *       然后根据自定义key的compareTo方法对key进行一个shuffle、merge的过程如<odh.2016-01,<1,1,1,1,1,1>
 *       然后再输出到reducer进行最终合并
 */
public class LogCount {
    //SelectData中生成文件的key,value就是Text，Text，所以用同样的格式读取
    public static class LogCountMapper extends Mapper<Text, Text, MemberLogTime, IntWritable> {
        private MemberLogTime mt = new MemberLogTime();
        private IntWritable one = new IntWritable(1);
        enum LogCounter {
            January,
            February
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String member_name = key.toString();
            String logTime = value.toString();
            //计算器计算1、2月份分别输入总记录条数，即统计当月总访问次数
            if (logTime.contains("2016-01")) {
                context.getCounter(LogCounter.January).increment(1);
            } else {
                context.getCounter(LogCounter.February).increment(1);
            }
            mt.setMember_name(member_name);
            mt.setLogTime(logTime);
            context.write(mt, one);
        }
    }

    public static class LogCountCombiner extends Reducer<MemberLogTime, IntWritable, MemberLogTime, IntWritable> {
        @Override
        protected void reduce(MemberLogTime key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            先在本地对两个区(partitioner后)做一次合并，减少IO操作数据量，提高效率
//            所以同样的<odh.2020-01,1>和<odh.2020-02,1>并不会因为member.name相同而融合成一个key，因为他们不在一个区里面
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class LogCountReducer extends Reducer<MemberLogTime, IntWritable, MemberLogTime, IntWritable> {
        @Override
        protected void reduce(MemberLogTime key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //计数器计算1、2月份分别输出条数，即统计当月访问用户量
            if (key.getLogTime().contains("2016-01")) {

                context.getCounter("OutputCounter", "JanuaryResult").increment(1);
            } else
                context.getCounter("OutputCounter", "February").increment(1);
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static class LogCountPartitioner extends Partitioner<MemberLogTime, IntWritable> {

        @Override
        //  numPartitions为作业的reducer任务数，此处为2，即可以去0，1
        public int getPartition(MemberLogTime key, IntWritable value, int numPartitions) {
            String date = key.getLogTime();
            if (date.contains("2016-01")) {//1月份对应输出文件part-r(reduce)-00000  第1个reduceTasks
                return 0 % numPartitions;
            } else                          //2月份对应输出文件part-r(reduce)-00001  第2个reduceTasks
                return 1 % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("error <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "log count");
        job.setJarByClass(LogCount.class); //驱动类
        job.setNumReduceTasks(2); //设置Reducer个数
        job.setMapperClass(LogCountMapper.class);//设置Mapper类
        job.setCombinerClass(LogCountCombiner.class);//设置Combiner类
        job.setReducerClass(LogCountReducer.class);//设置Reducer类
        job.setPartitionerClass(LogCountPartitioner.class);//设置Partitioner类

        job.setOutputKeyClass(MemberLogTime.class); //k4
        job.setOutputValueClass(IntWritable.class); //v4
        job.setInputFormatClass(SequenceFileAsTextInputFormat.class); //读取序列化文件(因为在SelectData输出的时候使用了SequenceFileOutputFormat）
        job.setOutputFormatClass(TextOutputFormat.class);//默认输出格式  输出文件

        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //设置输入文件的路径

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); //设置输出文件的路径


        System.exit(job.waitForCompletion(true) ? 0 : 1);// 执行

    }
}
