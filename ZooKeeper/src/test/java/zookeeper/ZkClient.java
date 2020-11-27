package zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


public class ZkClient {

    private ZooKeeper zooKeeper;

    @Before
    public void before() throws IOException {
        //1. 创建对象
        zooKeeper = new ZooKeeper(
                "hadoop102:2181,hadoop103:2181,hadoop104:2181",
                2000,
                new Watcher() {
                    public void process(WatchedEvent event) {
                        System.out.println("默认的回调函数");
                    }
                }
        );
    }

    @After
    public void after() throws InterruptedException {

        //3. 关闭
        zooKeeper.close();
    }

    @Test
    public void create() throws IOException, KeeperException, InterruptedException {

        //2. 做事情
        zooKeeper.create("/abc",
                "ceshi".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
    }


    @Test
    public void set() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(
                "/abc0000000007",
                new Watcher() {
                    public void process(WatchedEvent event) {
                        System.out.println("自定义回调函数");
                    }
                }
        );
        if (stat != null) {
            zooKeeper.setData(
                    "/abc0000000007",
                    "ceshiwanle2".getBytes(),
                    stat.getVersion()
            );
        }
    }

    @Test
    public void get() throws KeeperException, InterruptedException, IOException {
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData("/abc0000000007", true, stat);

        System.out.write(data);

        System.out.println();
    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/", true);
        children.forEach(
                System.out::println
        );
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        zooKeeper.delete("/abc0000000007", 2);
    }

    public void getChildren() {
        try {
            List<String> children = zooKeeper.getChildren("/", new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    getChildren();
                }
            });

            System.out.println("====================================");
            children.forEach(System.out::println);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getTest() throws InterruptedException {
        getChildren();
        Thread.sleep(Long.MAX_VALUE);
    }

}
