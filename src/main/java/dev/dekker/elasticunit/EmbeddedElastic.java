/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package dev.dekker.elasticunit;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PORT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING;

/**
 * EmbeddedElastic is meant to run an elastic node inside a jvm, without all the perks from {@link org.elasticsearch.test.ESIntegTestCase}.
 * This class is inspired by {@link org.elasticsearch.test.InternalTestCluster}.
 */
final class EmbeddedElastic {

    static {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
    }

    private final Logger logger = Loggers.getLogger(getClass());

    private static final int PORTS_PER_CLUSTER = 20;

    private static final int TRANSPORT_BASE_PORT = 9300;
    private static final int HTTP_BASE_PORT = 19200;

    private static final Collection<Class<? extends Plugin>> plugins = asList(Netty4Plugin.class, CommonAnalysisPlugin.class, ReindexPlugin.class);

    private final Path baseDir;

    private final Settings settings;

    private Node node;

    EmbeddedElastic(Path baseDir, String clusterName) {
        this.baseDir = baseDir;
        this.settings = buildSettings(baseDir, clusterName);
    }

    private Settings buildSettings(Path baseDir, String clusterName) {
        Builder builder = Settings.builder();
        builder.put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), Integer.MAX_VALUE);
        builder.put(Environment.PATH_SHARED_DATA_SETTING.getKey(), baseDir.resolve("custom"));
        builder.put(Environment.PATH_HOME_SETTING.getKey(), baseDir);
        builder.put(Environment.PATH_REPO_SETTING.getKey(), baseDir.resolve("repos"));
        builder.put(TcpTransport.PORT.getKey(), TRANSPORT_BASE_PORT + "-" + (TRANSPORT_BASE_PORT + PORTS_PER_CLUSTER));
        builder.put(SETTING_HTTP_PORT.getKey(), HTTP_BASE_PORT + "-" + (HTTP_BASE_PORT + PORTS_PER_CLUSTER));
        builder.put(SETTING_PIPELINING.getKey(), true);
        // Default the watermarks to absurdly low to prevent the tests
        // from failing on nodes without enough disk space
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b");
        // Some tests make use of scripting quite a bit, so increase the limit for integration tests
        builder.put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE.getKey(), "1000/1m");
        builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 3);
        builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 3);
        // always reduce this - it can make tests really slow
        builder.put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.getKey(), TimeValue.timeValueMillis(35));
        builder.put(Transport.TRANSPORT_TCP_COMPRESS.getKey(), false);
        builder.put(EsExecutors.PROCESSORS_SETTING.getKey(), 1);
        builder.put(NetworkModule.HTTP_ENABLED.getKey(), true);
        builder.put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName);

        builder.put(Environment.PATH_HOME_SETTING.getKey(), baseDir);
        builder.put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), 1L);

        builder.put(Node.NODE_NAME_SETTING.getKey(), clusterName);
        builder.put(Node.NODE_MASTER_SETTING.getKey(), true);
        builder.put(Node.NODE_DATA_SETTING.getKey(), true);
        return builder.build();
    }

    synchronized Client client() {
        return node.client();
    }

    synchronized void reset() throws IOException {
        stopNodesAndClients();
        wipeDataDirectories();

        node = new MockNode(settings, plugins);
        startNode();
        validateClusterFormed();
        logger.debug("Cluster is started - node: [{}]", node);
    }

    private void startNode() {
        try {
            node.start();
        } catch (NodeValidationException e) {
            throw new RuntimeException(e);
        }
    }

    private void validateClusterFormed() {
        final int size = 1;
        logger.trace("validating cluster formed via [{}], expecting [{}]", node, size);
        final Client client = client();
        ClusterHealthResponse response = client.admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(size)).get();
        if (response.isTimedOut()) {
            logger.warn("failed to wait for a cluster of size [{}], got [{}]", size, response);
            throw new IllegalStateException("cluster failed to reach the expected size of [" + size + "]");
        }
    }

    private <T> T getInstance(Class<T> clazz) {
        return node.injector().getInstance(clazz);
    }

    InetSocketAddress httpAddresses() {
        return getInstance(HttpServerTransport.class).boundAddress().publishAddress().address();
    }

    void stopNodesAndClients() throws IOException {
        if (node != null) {
            try {
                Releasables.close(node.client());
            } finally {
                node.close();
            }
        }
    }

    private void wipeDataDirectories() throws IOException {
        if (Files.exists(baseDir)) {
            FileSystemUtils.deleteSubDirectories(baseDir);
            logger.info("Wiped data directory for node location: {}", baseDir);
        }
    }

}
