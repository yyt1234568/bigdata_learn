package iotek.mr.removerepeat;

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

/**
 * Created  on 2018/7/6.
 * @author Administrator
 */
public class RemoveRepeat {

    //
    public  static class RemoveRepeatMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

        private Text tWord = new Text();
        NullWritable nul = NullWritable.get();


        //Object key, Object value 默认情况下key表示一行内容的偏离值。
        // sentence指的是读入的一行内容
        @Override
        protected void map(LongWritable key, Text sentence, Context context) throws IOException, InterruptedException {
            String content = sentence.toString();
            String[] words = content.split(" ");
            for(String word:words){
                tWord.set(word);
                context.write(tWord, nul);
            }
        }
    }

    public static class RemoveRepeatReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
        NullWritable nul = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, nul);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //
        System.setProperty("hadoop.home.dir", "C:\\Users\\Administrator\\Desktop\\QQ\\hadoop2.6.5-bin");

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        //指出打包的类
        job.setJarByClass(RemoveRepeat.class);
        //mapper任务的类 和Reducer任务的类
        job.setMapperClass(RemoveRepeatMapper.class);
        job.setReducerClass(RemoveRepeatReducer.class);

        //指出map阶段分别对应的输出键值对的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //reduce阶段输出键值对的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //指出map阶段要处理数据的路径
        FileInputFormat.setInputPaths(job, new Path("D:\\input"));
        //指出reduce阶段处理结果的输出路径，必须是不存在的
        FileOutputFormat.setOutputPath(job, new Path("D:\\output"));

        //提交mr任务到集群，提交完成后返回
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
