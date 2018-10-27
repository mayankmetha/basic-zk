import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

class connection {
    private ZooKeeper zoo;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    ZooKeeper connect(String host)throws IOException {
        zoo = new ZooKeeper(host, 5000, watchedEvent -> {
            if(watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        return zoo;
    }

    void close()throws InterruptedException {
        zoo.close();
    }

}
