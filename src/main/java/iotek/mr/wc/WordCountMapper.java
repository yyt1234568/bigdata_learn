package iotek.mr.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入的键值对类型< 偏移量， 一行文字的内容 >  <单词， 1>输出的键值对类型
 * @author Administrator
 * Created on 2018/6/19.
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, LongWritable> {
    private LongWritable one = new LongWritable(1);
    private Text wordt = new Text();

    /**
     * 每读取一行文字，会调用该方法处理一次
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读到的一行文字内容
        String sentence = value.toString();
        String[] words = sentence.split(" ");
        for(String word:words){
            wordt.set(word);
            context.write(wordt, one);
        }
        

    }
}
