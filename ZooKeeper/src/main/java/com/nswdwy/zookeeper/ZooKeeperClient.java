package com.nswdwy.zookeeper;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author yycstart
 * @create 2020-09-12 10:32
 */
public class ZooKeeperClient {
    ZooKeeper zooKeeper;

    @Before
    public void before() throws IOException {
        zooKeeper = new ZooKeeper(
                "hadoop102:2181,hadoop103:2181,hadoop104:2181",
                2000,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        System.out.println("默认监听");
                    }
                }
        );
    }
    @After
    public void after() throws InterruptedException {
        zooKeeper.close();
    }

    @Test
    public void create() throws KeeperException, InterruptedException {

        zooKeeper.create("/wulin1", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    }
    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/", true);
        children.forEach(System.out::println);
    }
    @Test
    public void get() throws KeeperException, InterruptedException, IOException {
        byte[] data = zooKeeper.getData("/wulin1", true, new Stat());
//        System.out.println(Arrays.toString(data));
        System.out.write(data);
        System.out.println();
    }

    @Test
    public void set() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/wulin1", true);
        zooKeeper.setData("/wulin1","1212".getBytes(),stat.getVersion());
    }
    @Test
    public void getChildren(){
        try {
            List<String> children = zooKeeper.getChildren("/", new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    getChildren();
                }
            });
            System.out.println("------------------");
            children.forEach(System.out::println);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void delete() throws KeeperException, InterruptedException {
        Stat exists = zooKeeper.exists("/wulin1", true);
        int version = exists.getVersion();
        zooKeeper.delete("/wulin1", version);
    }


}
