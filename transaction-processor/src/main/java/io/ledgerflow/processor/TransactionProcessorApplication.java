package io.ledgerflow.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

@SpringBootApplication
public class TransactionProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransactionProcessorApplication.class, args);
    }

    @Bean
    RestClient providerRestClient(
        RestClient.Builder builder,
        org.springframework.core.env.Environment environment) {

        Duration connectTimeout = environment.getProperty(
            "ledgerflow.provider.connect-timeout", Duration.class, Duration.ofSeconds(1));
        Duration readTimeout = environment.getProperty(
            "ledgerflow.provider.read-timeout", Duration.class, Duration.ofSeconds(2));

        var httpClient = HttpClient.newBuilder()
            .connectTimeout(connectTimeout)
            .build();

        var requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(readTimeout);

        return builder
            .requestFactory(requestFactory)
            .baseUrl(environment.getRequiredProperty(
                "ledgerflow.provider.base-url"))
            .build();
    }
}
