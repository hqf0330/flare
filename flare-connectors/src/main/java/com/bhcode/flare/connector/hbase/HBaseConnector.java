package com.bhcode.flare.connector.hbase;

import com.bhcode.flare.common.util.PropUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public final class HBaseConnector {

    private static final Map<Integer, Connection> connections = new ConcurrentHashMap<>();

    private HBaseConnector() {
        // Utility class
    }

    public static String hbasePrefix(int keyNum) {
        return keyNum == 1 ? "hbase." : "hbase" + keyNum + ".";
    }

    /**
     * Get or create an HBase connection for the given keyNum.
     */
    public static Connection getConnection(int keyNum) throws IOException {
        if (connections.containsKey(keyNum)) {
            Connection conn = connections.get(keyNum);
            if (!conn.isClosed()) {
                return conn;
            }
        }

        synchronized (connections) {
            String prefix = hbasePrefix(keyNum);
            Configuration conf = HBaseConfiguration.create();
            
            String zkQuorum = PropUtils.getString(prefix + "zk.quorum");
            String zkPort = PropUtils.getString(prefix + "zk.port", "2181");
            String znodeParent = PropUtils.getString(prefix + "znode.parent", "/hbase");

            if (zkQuorum != null && !zkQuorum.isEmpty()) {
                conf.set("hbase.zookeeper.quorum", zkQuorum);
                conf.set("hbase.zookeeper.property.clientPort", zkPort);
                conf.set("zookeeper.znode.parent", znodeParent);
            }

            // Load extra props: hbase.props.*
            Map<String, String> extra = PropUtils.sliceKeys(prefix + "props.");
            extra.forEach(conf::set);

            log.info("Creating HBase connection for keyNum={}, quorum={}", keyNum, zkQuorum);
            Connection conn = ConnectionFactory.createConnection(conf);
            connections.put(keyNum, conn);
            return conn;
        }
    }

    public static TableName getTableName(int keyNum) {
        String name = PropUtils.getString(hbasePrefix(keyNum) + "table.name");
        return name != null ? TableName.valueOf(name) : null;
    }
}
