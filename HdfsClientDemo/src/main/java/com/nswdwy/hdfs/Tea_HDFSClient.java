package com.nswdwy.hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class Tea_HDFSClient {
    FileSystem fileSystem;

    @Before
    public void before() throws IOException, InterruptedException {
        //1. new一个对象，一般是我们要操作的框架的抽象对象
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication","1");

        fileSystem = FileSystem.get(
                URI.create("hdfs://hadoop102:9820"),configuration,"atguigu");
    }

    @After
    public void after() throws IOException {

        //3. 关闭资源
        fileSystem.close();
    }

    @Test
    public void get() throws IOException {


        //2. 用这个对象操作
        fileSystem.copyToLocalFile(
                new Path("/input/word.txt"),
                new Path("d:/")
        );

    }

    @Test
    public void put() throws IOException {
        fileSystem.copyFromLocalFile(
                new Path("D:/Z_Myself/HdbsTest/2.txt"),
                new Path("/1.txt")
        );
    }

    @Test
    public void touch() throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/newfile"));
        fsDataOutputStream.write("apache".getBytes());
        fsDataOutputStream.close();
    }

    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/testmkdir"));
    }

    @Test
    public void delete() throws IOException {
        fileSystem.delete(new Path("/testmkdir"), true);
    }

    @Test
    public void rename() throws IOException {
//        fileSystem.rename(
//                new Path("/input/word.txt"),
//                new Path("/word.txt")
//        );

        fileSystem.setPermission(
                new Path("/word.txt"),
                new FsPermission(511)
        );

    }

    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(
                new Path("/")
        );

        for (FileStatus fileStatus : fileStatuses) {
//            System.out.println("=======================================");
//            System.out.println(fileStatus.getPath());
//            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus);
        }


    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
                fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();

            System.out.println("=============================");
            System.out.println(locatedFileStatus.getPath());
            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();

            for (int i = 0; i < blockLocations.length; i++) {
                System.out.println("第" + i + "块");
                String[] hosts = blockLocations[i].getHosts();
                System.out.println(Arrays.toString(hosts));
            }

        }
    }

    @Test
    public void cat() throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path("/README.txt"));

        IOUtils.copyBytes(inputStream, System.out, 1024);

        IOUtils.closeStream(inputStream);
    }

}


