package dev.dekker.elasticunit;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.NetworkAddress;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class ElasticUnit {

    private static ElasticUnit instance;
    private final EmbeddedElastic embeddedElastic;
    private final RestClient restClient;

    private ElasticUnit(EmbeddedElastic embeddedElastic, RestClient restClient) {
        this.embeddedElastic = embeddedElastic;
        this.restClient = restClient;
    }

    public static void startEmbeddedElastic() {
        getInstance();
    }

    public static RestClient restClient() {
        return getInstance().restClient;
    }

    public static Client client() {
        return getInstance().embeddedElastic.client();
    }

    public static synchronized void shutdownElastic() throws IOException {
        getInstance().embeddedElastic.stopNodesAndClients();
        instance = null;
    }

    public static void refresh() {
        Client client = getInstance().embeddedElastic.client();
        client.admin().indices().prepareRefresh("_all").get();
    }

    public static void truncate() {
        Client client = getInstance().embeddedElastic.client();
        client.admin().indices().prepareDelete("_all").get();
    }

    private static synchronized ElasticUnit getInstance() {
        if (instance == null) {
            makeSureRandomizedTestingLibraryIsNotOnClasspath();
            EmbeddedElastic embeddedElastic = startNode();
            RestClient restClient = buildClient(embeddedElastic.httpAddresses());
            instance = new ElasticUnit(embeddedElastic, restClient);
        }
        return instance;
    }

    private static void makeSureRandomizedTestingLibraryIsNotOnClasspath() {
        try {
            Class.forName("com.carrotsearch.randomizedtesting.RandomizedContext");
            throw new IllegalStateException("EmbeddedElastic doesn't work with Randomized testing on the classpath, please exclude:\n" +
                    "configurations {\n" +
                    "   testCompile.exclude group: 'com.carrotsearch.randomizedtesting'\n" +
                    "}\n");
        } catch (ClassNotFoundException e) {
        }
    }

    private static EmbeddedElastic startNode() {
        Path baseDir = Paths.get("build/embeddedElastic");
        String clusterName = "embedded-test-cluster";

        EmbeddedElastic internalTestCluster = new EmbeddedElastic(baseDir, clusterName);
        try {
            internalTestCluster.reset();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return  internalTestCluster;
    }

    private static RestClient buildClient(InetSocketAddress httpAddress) {
        String hostAndPort = NetworkAddress.format(httpAddress);
        String host = hostAndPort.split(":")[0];
        int port  = Integer.parseInt(hostAndPort.split(":")[1]);
        return RestClient.builder(new HttpHost(host, port)).build();
    }

}
