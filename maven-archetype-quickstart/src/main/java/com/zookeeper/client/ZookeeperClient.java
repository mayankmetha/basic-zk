package com.zookeeper.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

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
            System.err.println("Failed to create sequential node " + nodeName + ": " + e.getMessage());
        }

        return null;
    }

    public String createEphemeralNode(String nodeName, String nodeData) {
        try {
            return this.zooKeeper.create(nodeName, nodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Failed to create ephemeral node " + nodeName + ": " + e.getMessage());
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

    public List<String> getChildren(String nodeName) {
        List<String> children = new ArrayList<>();
        try {
            children = this.zooKeeper.getChildren(nodeName, null);
            //System.out.println("Found " + children + " children for node " + nodeName);
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Failed to get children of node " + nodeName + ": " + e.getMessage());
        }

        return children;
    }

    public String getData(String nodeName) {
        try {
            return new String(this.zooKeeper.getData(nodeName, false, null));
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Failed to get node " + nodeName + ": " + e.getMessage());
        }

        return null;
    }

    public void watchChildren(String nodeName, Watcher watcher) {
        try {
            this.zooKeeper.getChildren(nodeName, watcher, null);
            //System.out.println("Successfully set watch");
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Failed to set watch on node " + nodeName + ": " + e.getMessage());
        }
    }

    public void close() {
        try {
            this.zooKeeper.close();
        } catch (InterruptedException e) {
            System.err.println("Failed to close handle: " + e.getMessage());
        }
    }

    public boolean isOpen() {
        return (this.zooKeeper.getState().compareTo(States.CONNECTED) == 0);
    }

    public void lock(String lockNode) {
        while (true) {
            if(null == createEphemeralNode(lockNode, Long.toString(ProcessHandle.current().pid()))) {
                try {
                Thread.sleep(100);
                } catch(InterruptedException e) {
                    System.out.println("Interrupted while locking!");
                    return;
                }
            } else {
                //System.out.println("Acquired lock");
                return;
            }
        }
    }

    public void unlock(String lockNode) {
        deleteNode(lockNode);
        //System.out.println("Released lock");
    }

    public int setData(String nodeName, String data) {
        try {
            this.zooKeeper.setData(nodeName, data.getBytes(), -1);
            return 0;
        } catch (KeeperException | InterruptedException e) {
            System.err.println("Failed to set data on node " + nodeName + ": " + e.getMessage());
        }
        return -1;
    }

    private ZooKeeper zooKeeper;
}