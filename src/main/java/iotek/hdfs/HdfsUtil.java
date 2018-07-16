package iotek.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Administrator on 2018/6/19.
 */
public class HdfsUtil {
    private static FileSystem fs;

    static{
        Configuration config = new Configuration();
        try {
            fs = FileSystem.get(new URI("hdfs://192.168.3.115:9000"), config, "yyt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private  static void  uploadFile(String local_name, String hdfs_name) throws IOException {
        fs.copyFromLocalFile(new Path(local_name), new Path(hdfs_name));
    }

    private static void downloadFile(String hdfs_name, String local_name) throws IOException {
        fs.copyToLocalFile(false, new Path(hdfs_name), new Path(local_name), true);
    }

    private static void mkdir() throws IOException {
        fs.mkdirs(new Path("/folder/myfolder"));
    }

    private static void listFiles() throws IOException {
        FileStatus[] fstatus_s = fs.listStatus(new Path("/"));
        for(FileStatus fstatus:fstatus_s){
            String name = fstatus.getPath().getName();
            if(fstatus.isDirectory()){
                System.out.println(name + " is dir");
            }else{
                System.out.println(name + " is file");
            }
        }
    }

    private static void listDetails() throws IOException {
        RemoteIterator<LocatedFileStatus> iters =  fs.listFiles(new Path("/"), true);
        while(iters.hasNext()){
            System.out.println("-----------------------------");
            LocatedFileStatus lfs = iters.next();
            System.out.println(lfs.getPath().getName());//文件名
            System.out.println(lfs.getOwner());//文件的作者
            System.out.println(lfs.getLen());//文件的长度

            BlockLocation[] bls = lfs.getBlockLocations();//
            listBlocksInfo(bls);
        }
    }

    private static  void listBlocksInfo(BlockLocation[] bls) throws IOException {
        StringBuffer sb = new StringBuffer();

        for(BlockLocation bl:bls){
            System.out.println("********************");
            sb.setLength(0);
            String[] hosts = bl.getHosts();
            for(String host: hosts){
                sb.append(host + "    ");
            }
            System.out.println(bl.getNames()[0] + "  " + bl.getNames()[1]);
            System.out.println(sb.toString());
            System.out.println(bl.getLength());
            System.out.println(bl.getTopologyPaths()[0]);
        }
    }

    private static void deleteDir() throws IOException {
        fs.delete(new Path("/folder/myfolder"), true);
    }
    public static void main(String[] args) throws IOException {
        uploadFile("D:\\bigdata.jar", "/input");
        //downloadFile("/asd.txt", "d:\\");

        //mkdir();
        //deleteDir();

        //listFiles();
        //listDetails();
        fs.close();
    }
}
