package com.zookeeper.client;

import java.io.IOException;
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

        if (args.length != 3) {
            System.err.println("Usage: <exe> <zookeeper servers> <application name> <start script>");
            return;
        }

        applicationName = args[1];
        starterScript = args[2];
        zk = new ZookeeperClient(args[0], null);

        if (!zk.nodeExists(parentNode)) {
            String createdNode = zk.createPersistentNode(parentNode, Long.toString(ProcessHandle.current().pid()));
            // System.out.println("Created Node " + createdNode);
        }

        // Create sequential node with data = pidToMonitor
        createdNode = zk.createNode(nodeName, Long.toString(ProcessHandle.current().pid()));
        // System.out.println("Created node " + createdNode + " with data " +
        // pidToMonitor);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shutdownCalled = true;
                System.out.println("Shutting down...");
                zk.close();
            }
        });

        identify();
        if (isMaster) {
            long pidToMonitor = getProcessToMonitor();
            if (pidToMonitor < 0) {
                System.err.println("Failed to find process to monitor");
                zk.close();
                System.exit(1);
            }
            System.out.println("Monitoring application pid " + pidToMonitor);

            // The root node data will be set with the pid to monitor always
            if (zk.setData(parentNode, Long.toString(pidToMonitor)) < 0) {
                System.err.println("Failed to set data on the parent");
                zk.close();
                System.exit(1);
            }
        } else {

        }

        zk.watchChildren(parentNode, new MasterNodeMonitor());

        while (true) {
            Thread.sleep(10000);
            String pid = zk.getData(parentNode);
            long pidToMonitor = -1;
            try {
                pidToMonitor = Long.parseLong(pid);
            } catch (NumberFormatException e) {
                System.err.println("Failed to get PID to monitor: " + e.getMessage());
                zk.close();
                System.exit(1);
            }

            zk.lock(nodeLock);
            if (!isApplicationAlive(pidToMonitor)) {
                System.out.println("Application " + pidToMonitor + " has stopped");
                zk.close();
                zk.unlock(nodeLock);
                return;
            }
            zk.unlock(nodeLock);
        }
    }

    public static void identify() {
        List<String> nodeNames = new ArrayList<>();
        for (String node : zk.getChildren(parentNode)) {
            // System.out.println("Found child node " + node + " with data: " + nodeData);
            if (node.contains(nodePrefix)) {
                nodeNames.add(parentNode + "/" + node);
            }
        }

        nodeNames.sort(Comparator.naturalOrder());
        // System.out.println("The node names are: " + nodeNames);
        if (nodeNames.size() > 0) {
            if (nodeNames.get(0).equals(createdNode)) {
                System.out.println("The application " + applicationName + " with process ID " + zk.getData(createdNode)
                        + " is master");
                isMaster = true;
            } else {
                System.out.println("The application " + applicationName + " with process ID " + zk.getData(createdNode)
                        + " is slave");
                isMaster = false;
            }
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
                String pid = zk.getData(parentNode);
                long pidToMonitor = -1;
                try {
                    pidToMonitor = Long.parseLong(pid);
                } catch (NumberFormatException e) {
                    System.err.println("Failed to get PID to monitor: " + e.getMessage());
                    zk.close();
                    System.exit(1);
                }

                if (isMaster && !isApplicationAlive(pidToMonitor)) {
                    try {
                        zk.lock(nodeLock);
                        Runtime.getRuntime().exec(starterScript);
                        System.out.println("Executed script to restart the application");
                        // Update the data in parent
                        Thread.sleep(2000);
                        pidToMonitor = getProcessToMonitor();
                        if (pidToMonitor < 0) {
                            System.err.println("The application failed to start");
                        } else {
                            System.out.println("The application has started successfully. New PID: " + pidToMonitor);
                            zk.setData(parentNode, Long.toString(pidToMonitor));
                        }
                    } catch (IOException | InterruptedException e) {
                        System.err.println("Failed to start the application: " + e.getMessage());
                    } finally {
                        zk.unlock(nodeLock);
                    }
                }
                zk.watchChildren(parentNode, this);
            }
        }
    }

    public static long getProcessToMonitor() {
        Set<ProcessHandle> matchingProcesses = ProcessHandle.allProcesses()
                .filter(process -> process.info().commandLine().isPresent())
                .filter(process -> process.info().commandLine().get().endsWith(applicationName))
                .filter(process -> process.pid() != ProcessHandle.current().pid()).collect(Collectors.toSet());
        // System.out.println("Found " + matchingProcesses.size() + " processes running
        // under " + applicationName);

        if (matchingProcesses.isEmpty()) {
            return -1;
        }

        // To prevent any race conditions, get a lock before selecting application to
        // monitor
        zk.lock(nodeLock);
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

        zk.unlock(nodeLock);
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
    final static String nodeLock = parentNode + "/" + "lock";
    private static boolean isMaster = false;
    private static String applicationName = null;
    private static String starterScript = null;
    private static ZookeeperClient zk;
    private static String createdNode = null;
    private static boolean shutdownCalled = false;

}