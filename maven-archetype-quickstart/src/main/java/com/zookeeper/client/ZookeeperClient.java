package com.zookeeper.client;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperClient {
    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1:2181", 3000, null);
        System.out.println("Hello, Zookeeper!");

        if (null != zooKeeper.exists("/zookeper_demo", null)) {
            zooKeeper.delete("/zookeper_demo", 0);
        }

        System.out.println("Created Zookeeper node: " + zooKeeper.create("/zookeper_demo", "Testing Code".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        System.out.println("PID: " + ProcessHandle.current().pid());
        zooKeeper.close();
    }
}