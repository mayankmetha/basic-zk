package com.zookeeper.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ClusterNode {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: <exe> <zookeeper servers> <number of nodes>");
            return;
        }

        try {
            numProcesses = Integer.parseInt(args[1]);
            if (numProcesses <= 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            System.err.println("Invalid number specified: " + args[1]);
        }

        ClusterNode.args = args;
        zk = new ZookeeperClient(args[0], null);

        if (!zk.nodeExists(parentNode)) {
            String createdNode = zk.createPersistentNode(parentNode, Long.toString(ProcessHandle.current().pid()));
            // System.out.println("Created Node " + createdNode);
        }

        createdNode = zk.createNode(nodeName, Long.toString(ProcessHandle.current().pid()));
        if (null == createdNode) {
            System.err.println("Creating the node failed");
            System.exit(1);
        }

        // System.out.println("Successfully created node: " + createdNode);
        identify();

        if (isMaster) {
            zk.lock(nodeLock);
            for (int i = 1; i < numProcesses; ++i) {
                startChild();
            }
            zk.unlock(nodeLock);
        }

        // Sleep for a few seconds to allow for node creation
        Thread.sleep(5000);
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
            // System.out.println("Found child node " + node + " with data: " +
            // zk.getData(parentNode + "/" + node));
            if (node.contains(nodePrefix)) {
                nodeNames.add(parentNode + "/" + node);
            }
        }

        nodeNames.sort(Comparator.naturalOrder());
        // System.out.println("The node names are: " + nodeNames);
        if (nodeNames.size() > 0) {
            if (nodeNames.get(0).equals(createdNode)) {
                System.out.println("The current node " + ProcessHandle.current().pid() + " is master");
                isMaster = true;
            } else {
                System.out.println("The current node " + ProcessHandle.current().pid() + " is slave");
                isMaster = false;
            }
        }
    }

    private static void startChild() {
        String command = System.getProperty("java.home") + "/bin/java " + "-classpath "
                + System.getProperty("java.class.path") + " " + ClusterNode.class.getCanonicalName();

        for (String arg : args) {
            command += " " + arg;
        }

        // System.out.println("Command: " + command);
        try {
            ProcessBuilder builder = new ProcessBuilder(command.split("\\s+"));
            builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            builder.redirectError(ProcessBuilder.Redirect.INHERIT);
            builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
            Process p = builder.start();
            // Process p = Runtime.getRuntime().exec(command);
            System.out.println("Started process: " + p.pid());
        } catch (IOException e) {
            System.err.println("Failed to start the process: " + e.getMessage());
        }
    }

    static class MasterNodeMonitor implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            // System.out.println("Event of type " + event.getType() + " received on node "
            // + event.getPath()
            // + ", current state of the node: " + event.getState());
            if (!shutdownCalled) {
                identify();
                if (isMaster) {
                    zk.lock(nodeLock);
                    // Start n -1 processes
                    List<String> children = zk.getChildren(parentNode);
                    int numRunning = 0;
                    for (String child : children) {
                        if (child.contains(nodePrefix)) {
                            ++numRunning;
                        }
                    }
                    System.out.println("Found " + numRunning + " children running");

                    // Start the difference of the processes
                    for (int i = numRunning; i < numProcesses; ++i) {
                        startChild();
                    }
                    zk.unlock(nodeLock);
                }
            }
            zk.watchChildren(parentNode, this);
        }
    }

    final static String parentNode = "/zookeeper_demo";
    final static String nodePrefix = "cluster_";
    final static String nodeName = parentNode + "/" + nodePrefix;
    final static String nodeLock = parentNode + "/" + "lock";
    private static boolean isMaster = false;
    private static int numProcesses = 0;
    private static String[] args = new String[0];
    private static ZookeeperClient zk;
    private static String createdNode = null;
    private static boolean shutdownCalled = false;

}