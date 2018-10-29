import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

public class app {

    private static String myID;

    private static void createRoot() {
        String path = "/BasicZNodeRoot";
        String host = "localhost";
        byte[] data = "basic-zk:".getBytes();
        try {
            zNode con = new zNode(host,null);
            if(!con.isExisting(path)) {
                myID = con.createPersistant(path,data);
            }
            con.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        createChild();
    }

    private static void createChild() {
        String path = "/BasicZNodeRoot/child";
        String host = "localhost";
        byte[] data = "basic-zk:".getBytes();
        try {
            zNode con = new zNode(host,null);
            if(con.isExisting(path)) {
                System.out.println(path+" exist");
            } else {
                System.out.println("Creating "+path);
                myID = con.createEphemeralSeq(path,data);
                String id[] = myID.split("/");
                myID = id[id.length -1];
                getChildren();
                try {
                    Thread.sleep(10000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
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
                    children.sort(Comparator.naturalOrder());
                    if(myID.equals(children.get(0))) {
                        System.out.println("I'm the master");
                    } else {
                        System.out.println("I'm the slave");
                    }
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
        createRoot();
    }
}
