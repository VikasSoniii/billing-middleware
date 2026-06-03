package com.samsung.smp.billing_middleware.config;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.TimeUnit;

@Configuration
public class RestTemplateConfig {

    @Bean
    public RestTemplate restTemplate() {
        // 1. Configure timeouts using the new ConnectionConfig class
        ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setConnectTimeout(30000, TimeUnit.MILLISECONDS) // Connect timeout
                .setSocketTimeout(30000, TimeUnit.MILLISECONDS)  // Read/Response timeout
                .build();

        // 2. Create a connection manager and apply the config
        //    This is also where you would set your max connections.
        PoolingHttpClientConnectionManager connectionManager =
                PoolingHttpClientConnectionManagerBuilder.create()
                        .setDefaultConnectionConfig(connectionConfig)
                        .setMaxConnTotal(100)   // Total connections
                        .setMaxConnPerRoute(20) // Per-route connection limit
                        .build();

        // 3. Build the HttpClient with the connection manager
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();

        // 4. Create the factory and set the fully-configured HttpClient
        HttpComponentsClientHttpRequestFactory factory =
                new HttpComponentsClientHttpRequestFactory(httpClient);

        return new RestTemplate(factory);
    }
}