package com.vivekchutke.kafka.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchConsumerByVivek {

    public static RestHighLevelClient createClient() {
        //https://aky9btiw5p:cabsmlarnb@vivek-kafka-elastic-8700252773.us-west-2.bonsaisearch.net:443
        String hostName="vivek-kafka-elastic-8700252773.us-west-2.bonsaisearch.net";
        String userName="aky9btiw5p";
        String password="cabsmlarnb";

        // The below code is not rquired if you are using the local elastic search
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName,password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
            RestHighLevelClient client = new RestHighLevelClient(builder);
               return client;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerByVivek.class);
        RestHighLevelClient client = createClient();
        String testJsonString = "{\"Vivek\": \"Chutke\"}";

        IndexRequest indexRequest = new IndexRequest("twitter","tweets").source(testJsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info("Id is: "+id);
        client.close();
    }
}
