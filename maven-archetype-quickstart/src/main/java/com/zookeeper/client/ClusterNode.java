package com.zookeeper.client;

public class ClusterNode {
    public static void main(String[] args) throws Exception {
        ZookeeperClient zk = new ZookeeperClient("localhost:2181", null);

        if(!zk.nodeExists(parentNode)) {
            String createdNode = zk.createPersistentNode(parentNode, Long.toString(ProcessHandle.current().pid()));
        System.out.println("Created Node " + createdNode);
        }
        

        String createdNode = zk.createNode(nodeName, Long.toString(ProcessHandle.current().pid()));
        System.out.println("Successfully created node: " + createdNode);

        zk.close();

        Thread.sleep(10,000);
    }

    final static String parentNode = "/zookeeper_demo";
    final static String nodeName = parentNode + "/cluster_";
}