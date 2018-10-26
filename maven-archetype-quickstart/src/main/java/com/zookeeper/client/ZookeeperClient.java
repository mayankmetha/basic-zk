package com.zookeeper.client;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperClient {

    public ZookeeperClient(String hosts, Watcher watcher) throws IOException {
        this.zooKeeper = new ZooKeeper(hosts, 3000, watcher);
    }

    public boolean nodeExists(String nodeName) {
        try {
            if (null == this.zooKeeper.exists(nodeName, null)) {
                return false;
            }
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Failed to check if node exists: " + e.getMessage());
            return false;
        }

        return true;
    }

    public String createPersistentNode(String nodeName, String nodeData) {
        try {
            return this.zooKeeper.create(nodeName, nodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Failed to create persistent node " + nodeName + ": " + e.getMessage());
        }

        return null;
    }

    /**
     * Creates ephemeral sequential node
     */
    public String createNode(String nodeName, String nodeData) {
        try {
            return this.zooKeeper.create(nodeName, nodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Failed to create persistent node " + nodeName + ": " + e.getMessage());
        }

        return null;
    }

    public void deleteNode(String nodeName) {
        try {
            this.zooKeeper.delete(nodeName, 0);
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Failed to delete node " + nodeName + ": " + e.getMessage());
        }
    }

    public void close() {
        try {
            this.zooKeeper.close();
        } catch (InterruptedException e) {
            System.err.println("Failed to close handle: " + e.getMessage());
        }
    }

    private ZooKeeper zooKeeper;
}