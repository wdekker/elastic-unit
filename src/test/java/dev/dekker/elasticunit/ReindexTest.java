package dev.dekker.elasticunit;

import com.jayway.jsonpath.DocumentContext;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.jayway.jsonpath.JsonPath.parse;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static dev.dekker.elasticunit.ElasticUnit.restClient;
import static dev.dekker.elasticunit.ElasticUnit.startEmbeddedElastic;
import static dev.dekker.elasticunit.ShakespeareDataSetLoader.loadShakespeareDataSetOnlyOnce;

/**
 * Inspired by https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html
 */
public class ReindexTest {

    @BeforeClass
    public static void beforeClass() throws IOException {
        startEmbeddedElastic();
        loadShakespeareDataSetOnlyOnce();
    }

    @Test
    public void reindex() throws IOException {
        Request request = new Request(HttpPost.METHOD_NAME, "_reindex");
        request.setJsonEntity("{\n" +
                "  \"size\": 42,\n" +
                "  \"source\": {\n" +
                "    \"index\": \"shakespeare\"\n" +
                "  },\n" +
                "  \"dest\": {\n" +
                "    \"index\": \"new_shakespeare\"\n" +
                "  }\n" +
                "}");
        Response response = restClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(SC_OK));

        DocumentContext json = parse(response.getEntity().getContent());
        assertThat(json, hasJsonPath("$.created", is(42)));
    }

}
