import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class app {

    private static ZooKeeper zk;

    private static void create(String path, byte[] data)throws KeeperException, InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private static Stat isZNodeExisting(String path)throws KeeperException, InterruptedException {
        return zk.exists(path, true);
    }

    public static void main(String args[]) {

        // turn off log4j used by ZooKeeper
        Logger.getRootLogger().setLevel(Level.OFF);

        //cli:- delete /BasicZNode
        String path = "/BasicZNodeRoot";
        String host = "localhost";
        byte[] data = "basic-zk:".getBytes();
        try {
            connection con = new connection();
            zk = con.connect(host);
            if(isZNodeExisting(path) != null) {
                //ZNode exist
                System.out.println("ZNode exist");
            } else {
                System.out.println("Creating ZNode");
                create(path,data);
            }
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
