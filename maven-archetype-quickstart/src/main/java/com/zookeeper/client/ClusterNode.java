package com.zookeeper.client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ClusterNode {
    public static void main(String[] args) throws Exception {

        zk = new ZookeeperClient("localhost:2181", null);

        if (!zk.nodeExists(parentNode)) {
            String createdNode = zk.createPersistentNode(parentNode, Long.toString(ProcessHandle.current().pid()));
            System.out.println("Created Node " + createdNode);
        }

        createdNode = zk.createNode(nodeName, Long.toString(ProcessHandle.current().pid()));
        System.out.println("Successfully created node: " + createdNode);
        identify();
        zk.watchChildren(parentNode, new MasterNodeMonitor());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shutdownCalled = true;
                System.out.println("Shutting down...");
                zk.deleteNode(createdNode);
                zk.close();
            }
        });

        while (true) {
            Thread.sleep(10000);
        }
    }

    public static void identify() {
        List<String> nodeNames = new ArrayList<>();
        for (String node : zk.getChildren(parentNode)) {
            System.out.println("Found child node " + node + " with data: " + zk.getData(parentNode + "/" + node));
            if (node.contains(nodePrefix)) {
                nodeNames.add(parentNode + "/" + node);
            }
        }

        nodeNames.sort(Comparator.naturalOrder());
        System.out.println("The node names are: " + nodeNames);
        if (nodeNames.size() > 0) {
            if (nodeNames.get(0).equals(createdNode)) {
                System.out.println("The current node is master");
            } else {
                System.out.println("The current node is slave");
            }
        }
    }

    static class MasterNodeMonitor implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            System.out.println("Event of type " + event.getType() + " received on node " + event.getPath()
                    + ", current state of the node: " + event.getState());
            if (!shutdownCalled) {
                identify();
                zk.watchChildren(parentNode, this);
            }
        }
    }

    final static String parentNode = "/zookeeper_demo";
    final static String nodePrefix = "cluster_";
    final static String nodeName = parentNode + "/" + nodePrefix;
    private static ZookeeperClient zk;
    private static String createdNode = null;
    private static boolean shutdownCalled = false;

}