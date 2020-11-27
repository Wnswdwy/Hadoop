package com.nswdwy.hdfs;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;



import java.io.IOException;
import java.net.URI;


/**
 * @author yycstart
 * @create 2020-09-06 9:26
 */
public class HDFSClient {
    FileSystem fileSystem;

    @Before
    public void before() throws IOException, InterruptedException {
        //1. new 一个对象 ，一般是我们要操作的框架的对象
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","1");
        fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9820"),
                                    configuration,"atguigu");
    }
    @After
    public void after(){
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 下载 get
     * @throws IOException
     */
    @Test
    public void get() throws IOException {
        fileSystem.copyToLocalFile(new Path("/input/word.txt"),new Path("D:/3333.txt"));
    }

    /**
     * 上传 put
     * @throws IOException
     */
    @Test
    public void put() throws IOException {
        fileSystem.copyFromLocalFile(new Path("d:/Z_Myself/HdbsTest/333.txt"), new Path("/input/123.txt"));
    }

    /**
     * 创建文件夹（单层或多层） mkdir
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/123321/123"));
    }

    /**
     * 创建文件 touch
     * @throws IOException
     */
    @Test
    public void touch() throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/newfile"));
        fsDataOutputStream.write("asavsavasab".getBytes());
        fsDataOutputStream.close();
    }

    /**
     * 删除 rm（delete）
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
//        fileSystem.delete(new Path("/mytem.txt"),true);
        fileSystem.delete(new Path("/output2"), true);
    }

    /**
     * 显示列表list
     * @throws IOException
     */
    @Test
    public void listFile() throws IOException {
        RemoteIterator<LocatedFileStatus> locRemote = fileSystem.listFiles(
                                                new Path("/NOTICE.txt"), true);
        while (locRemote.hasNext()) {
            LocatedFileStatus locs = locRemote.next();
            System.out.println("------------------");
            System.out.println(locs.getPath());
            System.out.println(locs.getLen());
            System.out.println("=======================");
            BlockLocation[] blockLocations = locs.getBlockLocations();
            for (int i = 0; i < blockLocations.length; i++) {
                System.out.println();
            }

        }
    }


    /**
     * 显示cat
     * @throws IOException
     */
    @Test
    public void cat() throws IOException {
        FSDataInputStream open = fileSystem.open(new Path("/NOTICE.txt"));
        IOUtils.copyBytes(open,System.out, 1024);
        IOUtils.closeStream(open);
    }

    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/NOTICE.txt"));
        for(FileStatus fileStatus:fileStatuses){
            System.out.println(fileStatus.getPath());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getAccessTime());
            System.out.println(fileStatus.getBlockSize());

            System.out.println(fileStatus);
        }
    }



}
