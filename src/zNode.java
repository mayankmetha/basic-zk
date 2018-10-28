import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class zNode {
    private ZooKeeper zoo;

    zNode(String host, Watcher watcher)throws IOException {
        this.zoo = new ZooKeeper(host, 5000, watcher);
    }

    void close() {
        try {
            this.zoo.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    boolean isExisting(String path) {
        try {
            if(this.zoo.exists(path,null) == null) {
                return false;
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    void createPersistant(String path, byte[] data) {
        try {
            this.zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    void createEphemeral(String path, byte[] data) {
        try {
            this.zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    void deleteNode(String path) {
        try {
            this.zoo.delete(path, 0);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    List<String> getChildren(String path) {
        List<String> children = new ArrayList<>();
        try {
            children = this.zoo.getChildren(path,null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return children;
    }

}
