import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Scanner;

public class app {

    private static void createRoot(long pid) {
        //cli:- delete /BasicZNode
        String path = "/BasicZNodeRoot";
        String host = "localhost";
        byte[] data = "basic-zk:".getBytes();
        try {
            zNode con = new zNode(host,null);
            if(!con.isExisting(path)) {
                con.createPersistant(path,data);
            }
            con.close();
        } catch (Exception e) {
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void delete() {
        System.out.println("Enter path of ZNode");
        String path = new Scanner(System.in).nextLine();
        String host = "localhost";
        List<String> childList;
        try {
            zNode con = new zNode(host, null);
            childList = con.getChildren(path);
            if(childList.isEmpty()) {
                con.deleteNode(path);
                System.out.println("Delete success");
            } else {
                System.out.println("Cannot delete zNode with children");
            }
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {

        // turn off log4j used by ZooKeeper
        Logger.getRootLogger().setLevel(Level.OFF);

        //Get pid
        long pid = ProcessHandle.current().pid();
        System.out.println("PID: "+pid);


        //menu
        System.out.println("Select option:");
        System.out.println("1 -> Create");
        System.out.println("2 -> Delete");
        System.out.print("Input> ");
        int op = new Scanner(System.in).nextInt();

        switch (op) {
            case 1:
                createRoot(pid);
                break;
            case 2:
                delete();
                break;
        }
    }
}
