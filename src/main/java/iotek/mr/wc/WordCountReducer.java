package iotek.mr.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Administrator on 2018/6/19.
 */
/*
reducer阶段的输入键值对类型  就是  map阶段输出键值对的类型。
 */
                                              //输入键值对类型            输出键值对类型
public class WordCountReducer extends Reducer<Text, LongWritable,      Text, LongWritable>{
    private long count;
    private LongWritable countL = new LongWritable();

    @Override                //单词       {1，1，1，1，1....}
    protected void reduce(Text word, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        count = 0;
        Iterator<LongWritable> iters = values.iterator();
        while (iters.hasNext()){
            count = count + iters.next().get();
        }
        countL.set(count);

        context.write(word, countL);
    }
}
