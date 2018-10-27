package com.zookeeper.client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class MonitorApplication {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: <exe> <zookeeper servers> <application name>");
            return;
        }

        applicationName = args[1];
        zk = new ZookeeperClient(args[0], null);

        if (!zk.nodeExists(parentNode)) {
            String createdNode = zk.createPersistentNode(parentNode, Long.toString(ProcessHandle.current().pid()));
            System.out.println("Created Node " + createdNode);
        }

        long pidToMonitor = getProcessToMonitor();
        if (pidToMonitor < 0) {
            System.err.println("Failed to find process to monitor");
            return;
        }

        // Create sequential node with data = pidToMonitor
        createdNode = zk.createNode(nodeName, Long.toString(pidToMonitor));
        System.out.println("Created node " + createdNode + " with data " + pidToMonitor);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shutdownCalled = true;
                System.out.println("Shutting down...");
                zk.close();
            }
        });

        identify();
        zk.watchChildren(parentNode, new MasterNodeMonitor());

        while (true) {
            Thread.sleep(10000);
            if (!isApplicationAlive(pidToMonitor)) {
                System.out.println("Application " + pidToMonitor + " has stopped");
                zk.close();
                return;
            }
        }
    }

    public static void identify() {
        List<String> nodeNames = new ArrayList<>();
        for (String node : zk.getChildren(parentNode)) {
            String nodeData = zk.getData(parentNode + "/" + node);
            System.out.println("Found child node " + node + " with data: " + nodeData);
            if (node.contains(nodePrefix)) {
                nodeNames.add(parentNode + "/" + node);
            }
        }

        nodeNames.sort(Comparator.naturalOrder());
        System.out.println("The node names are: " + nodeNames);
        if (nodeNames.size() > 0) {
            if (nodeNames.get(0).equals(createdNode)) {
                System.out.println("The application " + applicationName + " with process ID " + zk.getData(createdNode)
                        + " is master");
            } else {
                System.out.println("The application " + applicationName + " with process ID " + zk.getData(createdNode)
                        + " is slave");
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

    public static long getProcessToMonitor() {
        Set<ProcessHandle> matchingProcesses = ProcessHandle.allProcesses()
                .filter(process -> process.info().commandLine().isPresent())
                .filter(process -> process.info().commandLine().get().endsWith(applicationName))
                .filter(process -> process.pid() != ProcessHandle.current().pid()).collect(Collectors.toSet());
        System.out.println("Found " + matchingProcesses.size() + " processes running under " + applicationName);

        if (matchingProcesses.isEmpty()) {
            return -1;
        }

        // To prevent any race conditions, get a lock before selecting application to
        // monitor
        zk.lock(lockNode);
        long pidToMonitor = -1;
        List<String> children = zk.getChildren(parentNode);
        if (children.isEmpty() || matchingProcesses.size() == 1) {
            // Pick the first PID as the one to monitor, since there are no applications
            // running yet
            pidToMonitor = matchingProcesses.iterator().next().pid();
        } else {
            System.out.println("There are currently " + children.size() + " child nodes");
            List<Long> childPids = new ArrayList<>();
            for (String child : children) {
                childPids.add(Long.parseLong(zk.getData(parentNode + "/" + child)));
            }

            // Find the first unmonitored process - each relevant node will be monitoring
            // one PID.
            // FIlter out those PIDs which are currently being monitored by some process
            Optional<ProcessHandle> process = matchingProcesses.stream().filter(p -> !childPids.contains(p.pid()))
                    .findFirst();
            if (process.isPresent()) {
                System.out.println("Found process " + process.get().pid() + " to monitor");
                pidToMonitor = process.get().pid();
            }
        }

        zk.unlock(lockNode);
        if (pidToMonitor < 0) {
            System.err.println("Failed to find node to monitor");
        }

        return pidToMonitor;
    }

    public static boolean isApplicationAlive(long pid) {
        Optional<ProcessHandle> process = ProcessHandle.of(pid);
        return (process.isPresent() ? process.get().isAlive() : false);
    }

    final static String parentNode = "/zookeeper_demo";
    final static String nodePrefix = "cluster_";
    final static String nodeName = parentNode + "/" + nodePrefix;
    final private static String lockNode = parentNode + "/lock";
    private static String applicationName = null;
    private static ZookeeperClient zk;
    private static String createdNode = null;
    private static boolean shutdownCalled = false;

}