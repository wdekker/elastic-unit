package dev.dekker.elasticunit;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.elasticsearch.client.Request;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

class ShakespeareDataSetLoader {

    private static AtomicBoolean DONE = new AtomicBoolean();

    synchronized static void loadShakespeareDataSetOnlyOnce() throws IOException {
        if (DONE.compareAndSet(false, true)) {
            loadDataSet();
        }
    }

    private static void loadDataSet() throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "shakespeare/_doc/_bulk");
        request.setEntity(
                new InputStreamEntity(SearchTest.class.getResourceAsStream("/shakespeare_6.0.json"),
                        ContentType.create("application/x-ndjson")));
        ElasticUnit.restClient().performRequest(request);
        ElasticUnit.refresh();
    }

}
