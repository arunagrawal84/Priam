/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.connection;

import static com.google.common.base.Preconditions.checkNotNull;

import com.netflix.priam.config.IConfiguration;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmxManagerImpl implements AutoCloseable, JmxManager {
    private static final Logger logger = LoggerFactory.getLogger(JmxManagerImpl.class);
    private static String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private static String SS_OBJECT_NAME = "org.apache.cassandra.db:type=StorageService";
    private MBeanServerConnection mbeanServer;
    private CompactionManagerMBean cmProxy;
    private FailureDetectorMBean fdProxy;
    private Object ssProxy;
    private JMXConnector jmxConnector = null;
    private ObjectName ssMbeanName;
    private Timer timer = new Timer();
    private boolean reconnectionScheduled = false;
    private long lastConnectionLookup = 0;
    private String cachedConnectionId = null;

    private JMXServiceURL jmxUrl;
    private IConfiguration config;

    public JmxManagerImpl(IConfiguration config) {
        this.config = config;
    }

    // Synchronized because of the timer manipulation
    private synchronized void scheduleReconnector() {
        if (reconnectionScheduled) return;

        int delay = 100; // delay for 100ms before trying to connect
        int period = config.getJmxConnectionMonitorPeriodInMs(); // repeat every 60 sec.
        timer.cancel();
        timer.purge();
        timer = new Timer();
        logger.info(
                "Scheduling JMXManagerImpl connection monitor with initial delay of {} ms and interval of {} ms.",
                delay,
                period);
        timer.scheduleAtFixedRate(
                new TimerTask() {
                    public void run() {
                        tryGetConnection(false);
                    }
                },
                delay,
                period);
        reconnectionScheduled = true;
    }

    // Synchronized because mixing beans is a bad plan
    private synchronized void doConnect() {
        ObjectName cmMbeanName, fdMbeanName;
        HashMap environment = new HashMap();
        try {
            jmxUrl =
                    new JMXServiceURL(
                            String.format(
                                    JmxManagerImpl.JMX_URL, "localhost", config.getJmxPort()));
            ssMbeanName = new ObjectName(JmxManagerImpl.SS_OBJECT_NAME);
            cmMbeanName = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
            fdMbeanName = new ObjectName(FailureDetector.MBEAN_NAME);
            String[] credentials = new String[] {config.getJmxUsername(), config.getJmxPassword()};
            environment.put(JMXConnector.CREDENTIALS, credentials);
        } catch (MalformedURLException | MalformedObjectNameException e) {
            String msg =
                    String.format(
                            "Failed to prepare the JMX connection to %s:%s",
                            "localhost", config.getJmxPort());
            logger.error(msg, e);
            return;
        }
        try {
            if (StringUtils.isEmpty(config.getJmxUsername())
                    || StringUtils.isEmpty(config.getJmxPassword()))
                jmxConnector = JMXConnectorFactory.connect(jmxUrl);
            else jmxConnector = JMXConnectorFactory.connect(jmxUrl, environment);

            mbeanServer = jmxConnector.getMBeanServerConnection();
            ssProxy = JMX.newMBeanProxy(mbeanServer, ssMbeanName, StorageServiceMBean.class);
            cmProxy = JMX.newMBeanProxy(mbeanServer, cmMbeanName, CompactionManagerMBean.class);
            fdProxy = JMX.newMBeanProxy(mbeanServer, fdMbeanName, FailureDetectorMBean.class);
            logger.info(String.format("JMX connection properly connected: %s", jmxUrl.toString()));
        } catch (Exception e) {
            String msg =
                    String.format(
                            "Failed to establish JMX connection to %s:%s",
                            "localhost", config.getJmxPort());
            logger.error(msg, e);
        }
    }

    /** Connect to JMX interface on the given host and port. */
    @Override
    public void connect() {
        scheduleReconnector();
    }

    @Override
    public boolean reConnect() {
        try {
            doConnect();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean isGossipActive() {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "Looks like the proxy is not connected");
        return ((StorageServiceMBean) ssProxy).isGossipRunning();
    }

    @Override
    public boolean isThriftActive() {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "Looks like the proxy is not connected");
        return ((StorageServiceMBean) ssProxy).isRPCServerRunning();
    }

    @Override
    public boolean isNativeTransportActive() {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "Looks like the proxy is not connected");
        return ((StorageServiceMBean) ssProxy).isNativeTransportRunning();
    }

    @Override
    public boolean isJoined() {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "Looks like the proxy is not connected");
        return ((StorageServiceMBean) ssProxy).isJoined();
    }

    @Override
    public synchronized void takeSnapshot(final String snapshotTag) throws Exception {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "Looks like the proxy is not connected");
        try {
            ((StorageServiceMBean) ssProxy).takeSnapshot(snapshotTag);
        } catch (Exception e) {
            logger.error(
                    "Error while taking snapshot {}. Asking Cassandra to clear snapshot to avoid accumulation of snapshots.",
                    snapshotTag);
            clearSnapshot(snapshotTag);
            throw e;
        }
    }

    @Override
    public void clearSnapshot(final String snapshotTag) throws Exception {
        if (StringUtils.isEmpty(snapshotTag)) {
            // Passing in null or empty string will clear all snapshots on the host
            throw new IllegalArgumentException("snapshotTag cannot be null or empty string");
        }
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "Looks like the proxy is not connected");
        ((StorageServiceMBean) ssProxy).clearSnapshot(snapshotTag);
    }

    @Override
    public void forceKeyspaceCompaction(String keyspace, String... columnFamilies)
            throws Exception {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "Looks like the proxy is not connected");
        ((StorageServiceMBean) ssProxy).forceKeyspaceCompaction(keyspace, columnFamilies);
    }

    @Override
    public void forceKeyspaceFlush(String keyspace, String... columnFamilies) throws Exception {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "Looks like the proxy is not connected");
        ((StorageServiceMBean) ssProxy).forceKeyspaceFlush(keyspace, columnFamilies);
    }

    @Override
    public String getPartitioner() {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "Looks like the proxy is not connected");
        return ((StorageServiceMBean) ssProxy).getPartitionerName();
    }

    @Override
    public List<String> getKeyspaces() {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "Looks like the proxy is not connected");
        return ((StorageServiceMBean) ssProxy).getKeyspaces();
    }

    @Override
    public Map<String, List<String>> getColumnfamilies() throws Exception {
        return null;
    }

    @Override
    public boolean tableExists(String ks, String cf) {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        try {
            String type = cf.contains(".") ? "IndexColumnFamilies" : "ColumnFamilies";
            String nameStr =
                    String.format(
                            "org.apache.cassandra.db:type=*%s,keyspace=%s,columnfamily=%s",
                            type, ks, cf);
            Set<ObjectName> beans = mbeanServer.queryNames(new ObjectName(nameStr), null);
            if (beans.isEmpty() || beans.size() != 1) {
                return false;
            }
            ObjectName bean = beans.iterator().next();
            JMX.newMBeanProxy(mbeanServer, bean, ColumnFamilyStoreMBean.class);
        } catch (MalformedObjectNameException | IOException e) {
            String errMsg =
                    String.format(
                            "ColumnFamilyStore for %s/%s not found: %s", ks, cf, e.getMessage());
            logger.warn(errMsg);
            return false;
        }
        return true;
    }

    @Override
    public String getCassandraVersion() {
        checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        return ((StorageServiceMBean) ssProxy).getReleaseVersion();
    }

    @Override
    public String getConnectionId(boolean useCache) throws IOException {
        // We call this method a _lot so we cache this by default
        if (!useCache
                || (System.currentTimeMillis()
                        > lastConnectionLookup + config.getJmxConnectionIdCacheTTL())) {
            this.cachedConnectionId = jmxConnector.getConnectionId();
            lastConnectionLookup = System.currentTimeMillis();
        }
        return this.cachedConnectionId;
    }

    private boolean tryGetConnection(boolean useCache) {
        return isConnectionAlive(useCache) || reConnect();
    }

    private boolean tryGetConnection() {
        return tryGetConnection(true);
    }

    @Override
    public boolean isConnectionAlive(boolean useCache) {
        if (jmxConnector == null) return false;

        try {
            String connectionId = getConnectionId(useCache);
            return null != connectionId && connectionId.length() > 0;
        } catch (IOException e) {
            logger.error("Exception in checking if connection is alive.", e);
        } catch (NullPointerException e) {
            logger.error("No connection to check", e);
        }
        return false;
    }

    /** Cleanly shut down by closing the JMX connection. */
    @Override
    public void close() throws Exception {
        logger.debug(String.format("close JMX connection to: %s", jmxUrl));
        try {
            jmxConnector.close();
        } catch (IOException e) {
            logger.warn("failed closing a JMX connection.", e);
        }
    }

    static void checkState(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }
}
