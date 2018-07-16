package iotek.mr.flow;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSort {

    public static class mapper extends Mapper<LongWritable,Text,FlowBean,NullWritable>{

        NullWritable nul=NullWritable.get();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] strings = s.split(",");
            FlowBean flowBean=new FlowBean();
            flowBean.setPhone(strings[0]);
            flowBean.setUpFlow(Long.parseLong(strings[1]));
            flowBean.setDownFlow(Long.parseLong(strings[2]));
            flowBean.setCountFlow(Long.parseLong(strings[3]));

            context.write(flowBean,nul);
        }
    }


    public static class reducer extends Reducer<FlowBean,NullWritable,NullWritable,FlowBean>{
        NullWritable nul=NullWritable.get();
        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(nul,key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\Users\\Administrator\\Desktop\\QQ\\hadoop2.6.5-bin");

        Configuration config = new Configuration();
        //config.setBoolean("fs.hdfs.impl.disable.cache", true);
        Job job = Job.getInstance(config);

        //指出打包的类
        job.setJarByClass(FlowSort.class);
        //mapper任务的类 和Reducer任务的类
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);

        //指出map阶段分别对应的输出键值对的类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //reduce阶段输出键值对的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBean.class);

        //指出map阶段要处理数据的路径
        FileInputFormat.setInputPaths(job, new Path("D:\\input1"));
        //指出reduce阶段处理结果的输出路径，必须是不存在的
        FileOutputFormat.setOutputPath(job, new Path("D:\\output1"));

        //提交mr任务到集群，提交完成后返回
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
