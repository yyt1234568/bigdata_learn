package iotek.mr.my;



import iotek.mr.flow.FlowBean;
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
import java.util.Iterator;

public class FlowRegion {

    public static class mapper extends Mapper<LongWritable,Text,Text,Text>{

        private Text tPhone=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] strings = s.split("\t");
            tPhone.set(strings[1]);
            context.write(tPhone,value);
        }
    }


    public static class reducer extends Reducer<Text,Text,NullWritable,Text>{
        private NullWritable nul=NullWritable.get();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iters = values.iterator();
            while (iters.hasNext()){
                Text line = iters.next();
                context.write(nul,line);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\Users\\Administrator\\Desktop\\QQ\\hadoop2.6.5-bin");

        Configuration config = new Configuration();
        //config.setBoolean("fs.hdfs.impl.disable.cache", true);
        Job job = Job.getInstance(config);

        //指出打包的类
        job.setJarByClass(FlowRegion.class);
        job.setPartitionerClass(RegionHashPartioner.class);
        job.setNumReduceTasks(6);


        //mapper任务的类 和Reducer任务的类
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);

        //指出map阶段分别对应的输出键值对的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //reduce阶段输出键值对的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //指出map阶段要处理数据的路径
        FileInputFormat.setInputPaths(job, new Path("D:\\input"));
        //指出reduce阶段处理结果的输出路径，必须是不存在的
        FileOutputFormat.setOutputPath(job, new Path("D:\\output3"));

        //提交mr任务到集群，提交完成后返回
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
