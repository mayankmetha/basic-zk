import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class app {

    private static void createRoot(long pid) {
        String path = "/BasicZNodeRoot";
        String host = "localhost";
        byte[] data = "basic-zk:".getBytes();
        try {
            zNode con = new zNode(host,null);
            if(!con.isExisting(path)) {
                con.createPersistant(path,data);
            }
            con.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        createChild(pid);
    }

    private static void createChild(long pid) {
        String path = "/BasicZNodeRoot/"+pid;
        String host = "localhost";
        byte[] data = "basic-zk:".getBytes();
        try {
            zNode con = new zNode(host,null);
            if(con.isExisting(path)) {
                System.out.println(path+" exist");
            } else {
                System.out.println("Creating "+path);
                con.createPersistant(path,data);
            }
            con.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getChildren() {
        String path = "/BasicZNodeRoot";
        String host = "localhost";
        List<String> children;
        try {
            zNode con = new zNode(host, null);
            if(con.isExisting(path)) {
                children = con.getChildren(path);
                if(!children.isEmpty()) {
                    System.out.println("Registered process: "+children.toString());
                } else {
                    System.out.println("No children node");
                }
            } else {
                System.out.println("Path doesnt exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {

        // turn off log4j used by ZooKeeper
        Logger.getRootLogger().setLevel(Level.OFF);

        //Get pid
        long pid = ProcessHandle.current().pid();
        System.out.println("PID: "+pid);

        createRoot(pid);
        getChildren();
    }
}
