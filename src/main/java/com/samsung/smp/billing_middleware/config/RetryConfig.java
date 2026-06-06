package com.samsung.smp.billing_middleware.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Collections;

@Configuration
public class RetryConfig {

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        // Exponential backoff: 100ms, 200ms, 400ms, 800ms...
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(100);
        backOffPolicy.setMultiplier(2.0);
        backOffPolicy.setMaxInterval(5000);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        // Retry 3 times
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(
                3,
                Collections.singletonMap(Exception.class, true)
        );
        retryTemplate.setRetryPolicy(retryPolicy);

        return retryTemplate;
    }
}