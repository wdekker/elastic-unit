package dev.dekker.elasticunit;

import com.jayway.jsonpath.DocumentContext;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.jayway.jsonpath.JsonPath.parse;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static dev.dekker.elasticunit.ElasticUnit.restClient;
import static dev.dekker.elasticunit.ElasticUnit.startEmbeddedElastic;

/**
 * Based on a sample from https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
 */
public class IndexTest {

    @BeforeClass
    public static void beforeClass() {
        startEmbeddedElastic();
    }

    @Test
    public void index() throws IOException {
        Request request = new Request(HttpPut.METHOD_NAME, "twitter/_doc/1");
        String document = "{\n" +
                "    \"user\" : \"kimchy\",\n" +
                "    \"post_date\" : \"2009-11-15T14:12:12\",\n" +
                "    \"message\" : \"trying out Elasticsearch\"\n" +
                "}";
        request.setJsonEntity(document);
        Response response = restClient().performRequest(request);

        assertThat(response.getStatusLine().getStatusCode(), is(SC_CREATED));

        Response getResponse = restClient().performRequest(new Request(HttpGet.METHOD_NAME, "twitter/_doc/1"));

        assertThat(getResponse.getStatusLine().getStatusCode(), is(SC_OK));
        DocumentContext json = parse(getResponse.getEntity().getContent());
        assertThat(json, hasJsonPath("$._source.user", is("kimchy")));
    }

}
