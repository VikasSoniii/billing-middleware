package com.samsung.smp.billing_middleware.consumer;

import com.samsung.smp.billing_middleware.dto.BillingMessage;
import com.samsung.smp.billing_middleware.entity.ExactSampleRecord;
import com.samsung.smp.billing_middleware.repository.ExactSampleRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
public class BillingConsumerService {

    @Autowired
    private ExactSampleRepository repository;

    @KafkaListener(topics = "tenant-ingestion-stats", groupId = "billing-group")
    public void consume(BillingMessage message) {
        log.info("Received billing message for tenant: {} - Samples: {}",
                message.getTenantId(), message.getSamplesCount());

        // Save exact sample record to database
        ExactSampleRecord record = ExactSampleRecord.builder()
                .tenantId(message.getTenantId())
                .sampleCount(message.getSamplesCount())
                .requestCount(message.getRequestCount())
                .cumulativeTotal(message.getCumulativeTotal())
                .windowStart(message.getTimeWindowStart())
                .windowEnd(message.getTimeWindowEnd())
                .createdAt(LocalDateTime.now())
                .build();

        repository.save(record);

        log.info("Saved exact sample record for tenant: {} - Cumulative total: {}",
                message.getTenantId(), message.getCumulativeTotal());
    }
}