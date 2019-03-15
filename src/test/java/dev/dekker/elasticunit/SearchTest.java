package dev.dekker.elasticunit;

import com.jayway.jsonpath.DocumentContext;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.jayway.jsonpath.JsonPath.parse;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static dev.dekker.elasticunit.ElasticUnit.restClient;
import static dev.dekker.elasticunit.ElasticUnit.startEmbeddedElastic;
import static dev.dekker.elasticunit.ShakespeareDataSetLoader.loadShakespeareDataSetOnlyOnce;

/**
 * Inspired by https://www.elastic.co/blog/a-practical-introduction-to-elasticsearch
 */
public class SearchTest {

    @BeforeClass
    public static void beforeClass() throws IOException {
        startEmbeddedElastic();
        loadShakespeareDataSetOnlyOnce();
    }

    @Test
    public void search() throws IOException {
        Request request = new Request(HttpGet.METHOD_NAME, "shakespeare/_search");
        request.setJsonEntity("{  \n" +
                "   \"query\":{  \n" +
                "      \"bool\":{  \n" +
                "         \"must\":[  \n" +
                "            {  \n" +
                "               \"match\":{  \n" +
                "                  \"play_name\":\"Antony\"\n" +
                "               }\n" +
                "            },\n" +
                "            {  \n" +
                "               \"match\":{  \n" +
                "                  \"speaker\":\"Demetrius\"\n" +
                "               }\n" +
                "            }\n" +
                "         ]\n" +
                "      }\n" +
                "   }\n" +
                "}");
        Response response = restClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(SC_OK));

        DocumentContext json = parse(response.getEntity().getContent());
        assertThat(json, hasJsonPath("$.hits.total", greaterThan(0)));
        assertThat(json, hasJsonPath("$.hits.hits[0]._source.play_name", containsString("Antony")));
        assertThat(json, hasJsonPath("$.hits.hits[0]._source.speaker", equalToIgnoringCase("Demetrius")));
    }

    @Test
    public void aggregation() throws IOException {
        Request request = new Request(HttpGet.METHOD_NAME, "shakespeare/_search");
        request.setJsonEntity("{\n" +
                "    \"size\":0,\n" +
                "    \"aggs\" : {\n" +
                "        \"Total plays\" : {\n" +
                "            \"cardinality\" : {\n" +
                "                \"field\" : \"play_name.keyword\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}");
        Response response = restClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(SC_OK));

        DocumentContext json = parse(response.getEntity().getContent());
        assertThat(json, hasJsonPath("$.hits.total", greaterThan(0)));
        assertThat(json, hasJsonPath("$.aggregations['Total plays'].value", is(36)));
    }


}
