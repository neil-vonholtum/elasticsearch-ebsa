package com.neil.esebsa;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2ProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2ProxyResponseEvent;

import org.apache.http.HttpRequestInterceptor;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.apache.http.HttpHost;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class EsEbsa{

    private static final RestHighLevelClient esClient;
    private static final Map<String, String> headers = new HashMap<>();

    static {
        String aesEndpoint = "https://search-ebsa-n4h2b4ttgfyvw3l5agwgv7sjrq.us-west-1.es.amazonaws.com";
        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName("es");
        signer.setRegionName("us-west-1");
        HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor("es", signer, credentialsProvider);
        esClient = new RestHighLevelClient(RestClient.builder(HttpHost.create(aesEndpoint)).setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
        headers.put("Access-Control-Allow-Origin", "*");
    }

    public APIGatewayV2ProxyResponseEvent handler(APIGatewayV2ProxyRequestEvent input) throws IOException {

        SearchRequest searchRequest = new SearchRequest("ebsa");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.multiMatchQuery(input.getQueryStringParameters().get("q"), "PLAN_NAME", "SPONSOR_DFE_NAME", "SPONS_DFE_LOC_US_STATE"));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);

        APIGatewayV2ProxyResponseEvent apiGatewayV2ProxyResponseEvent = new APIGatewayV2ProxyResponseEvent();
        apiGatewayV2ProxyResponseEvent.setStatusCode(searchResponse.status().getStatus());
        apiGatewayV2ProxyResponseEvent.setHeaders(headers);
        apiGatewayV2ProxyResponseEvent.setBody(Arrays.toString(searchResponse.getHits().getHits()));
        return apiGatewayV2ProxyResponseEvent;
    }
}
