package iotek.mr.my;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.util.HashMap;
import java.util.Map;

public class RegionHashPartioner extends HashPartitioner<Text,Text> {
    private static Map<String,Integer> map=new HashMap<String, Integer>();

    static {
        map.put("135",0);
        map.put("136",1);
        map.put("137",2);
        map.put("138",3);
        map.put("139",4);
    }

    @Override
    public int getPartition(Text phone, Text line, int numReduceTasks) {
        String s = phone.toString();
        String ph = s.substring(0, 3);
        Integer region=map.get(ph);

        if (region!=null){
            return region;
        }
        return 5;

    }
}
