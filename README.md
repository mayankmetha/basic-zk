# basic-zk
**High Availability and Fault Tolerance using Zookeeper**

Download and install zookeeper.

Building the code:
Download and install maven.
Use "mvn package -DskipTests" to build the code. A jar called <artifactid>-jar-with-dependencies.jar is created under the "target" folder.

Ensure zookeeper server is running before attempting to run this code.

1. Basic FT: Restart applications if they have terminated
Running the application:
java -cp target/maven-archetype-quickstart-1.0-jar-with-dependencies.jar com.zookeeper.client.ClusterNode <zookeeper servers> <num processes in cluster>
For e.g.:
java -cp target/maven-archetype-quickstart-1.0-jar-with-dependencies.jar com.zookeeper.client.ClusterNode localhost:2181 3

This will start 3 processes, out of which exactly 1 is master, and the rest are slaves.
Kill one of the slaves - the master node will create another node to replace the one that expired.
Kill the master node - one of the remaining slaves becomes the master, and creates new applications to replace the ones that have terminated.

2. Monitoring an external application:
Choose an application to monitor, and start it up. In this case, it will be "node index.js"
Create a script to start the application, say for e.g. startServer.sh

Start the program through:
java -cp target/maven-archetype-quickstart-1.0-jar-with-dependencies.jar com.zookeeper.client.MonitorApplication <zookeeper servers> <application name> <script to start the application>

For e.g.:
java -cp target/maven-archetype-quickstart-1.0-jar-with-dependencies.jar com.zookeeper.client.MonitorApplication localhost:2181 "node index.js" ~/startServer.sh

Start multiple instances of the program, and one of them will become the master, the rest act as slaves.
If the application being monitored expires, then the master terminates itself. One of the remaining nodes becomes the master, and starts the application using the script provided.
