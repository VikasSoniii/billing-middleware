package com.samsung.smp.billing_middleware.service;
import com.google.protobuf.InvalidProtocolBufferException;
import com.samsung.smp.billing_middleware.dto.BillingMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import prometheus.Remote.WriteRequest;
import prometheus.Remote.TimeSeries;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class ExactSampleCounterService {
    @Value("${billing.kafka.topic:tenant-ingestion-stats}")
    private String kafkaTopic;

    @Autowired
    private KafkaTemplate<String, BillingMessage> kafkaTemplate;

    // Store exact sample counts per tenant
    private final Map<String, AtomicLong> exactSampleCounts = new ConcurrentHashMap<>();
    private final Map<String, TenantBatch> batchQueue = new ConcurrentHashMap<>();

    /**
     * Count samples exactly from Prometheus remote write request
     * @param tenantId Tenant identifier from X-Scope-OrgID header
     * @param requestBody Raw protobuf request body
     * @return Exact number of samples counted
     */
    public long countExactSamples(String tenantId, byte[] requestBody) {
        try {
            // Parse Prometheus remote write protobuf
            WriteRequest writeRequest = WriteRequest.parseFrom(requestBody);

            long exactSampleCount = 0;

            // Count every sample across all time series
            for (TimeSeries timeSeries : writeRequest.getTimeseriesList()) {
                exactSampleCount += timeSeries.getSamplesCount();
            }

            // Update exact counter for this tenant
            AtomicLong counter = exactSampleCounts.computeIfAbsent(tenantId,
                    k -> new AtomicLong(0));
            long cumulativeTotal = counter.addAndGet(exactSampleCount);

            // Add to batch queue for Kafka
            addToBatchQueue(tenantId, exactSampleCount, cumulativeTotal);

            log.debug("Counted {} exact samples for tenant: {} (Cumulative: {})",
                    exactSampleCount, tenantId, cumulativeTotal);

            return exactSampleCount;

        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse Prometheus remote write request for tenant: {}", tenantId, e);
            return 0;
        }
    }

    /**
     * Add to batch queue for periodic flushing to Kafka
     */
    private void addToBatchQueue(String tenantId, long samplesInRequest, long cumulativeTotal) {
        TenantBatch batch = batchQueue.computeIfAbsent(tenantId, k -> new TenantBatch());

        synchronized (batch) {
            batch.totalSamples += samplesInRequest;
            batch.requestCount++;
            batch.lastUpdateTime = System.currentTimeMillis();
            batch.latestCumulativeTotal = cumulativeTotal;
        }
    }

    /**
     * Flush batch to Kafka (called by scheduler)
     */
    public void flushBatchesToKafka() {
        batchQueue.forEach((tenantId, batch) -> {
            synchronized (batch) {
                if (batch.totalSamples > 0) {
                    BillingMessage message = BillingMessage.builder()
                            .tenantId(tenantId)
                            .samplesCount(batch.totalSamples)
                            .requestCount(batch.requestCount)
                            .cumulativeTotal(batch.latestCumulativeTotal)
                            .timestamp(Instant.now())
                            .timeWindowStart(LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(batch.windowStartTime),
                                    ZoneId.systemDefault()))
                            .timeWindowEnd(LocalDateTime.now())
                            .build();

                    // Send to Kafka
                    kafkaTemplate.send(kafkaTopic, tenantId, message);

                    log.info("Flushed {} exact samples for tenant: {} ({} requests, cumulative: {})",
                            batch.totalSamples, tenantId, batch.requestCount, batch.latestCumulativeTotal);

                    // Reset batch
                    batch.reset();
                    batch.windowStartTime = System.currentTimeMillis();
                }
            }
        });
    }

    /**
     * Get exact sample count for a tenant
     */
    public long getExactSampleCount(String tenantId) {
        AtomicLong counter = exactSampleCounts.get(tenantId);
        return counter != null ? counter.get() : 0;
    }

    /**
     * Get current batch sizes for monitoring
     */
    public Map<String, Integer> getBatchSizes() {
        Map<String, Integer> sizes = new ConcurrentHashMap<>();
        batchQueue.forEach((k, v) -> {
            synchronized (v) {
                sizes.put(k, v.requestCount);
            }
        });
        return sizes;
    }

    /**
     * Inner class for batch tracking
     */
    private static class TenantBatch {
        long totalSamples = 0;
        int requestCount = 0;
        long lastUpdateTime = System.currentTimeMillis();
        long windowStartTime = System.currentTimeMillis();
        long latestCumulativeTotal = 0;

        void reset() {
            totalSamples = 0;
            requestCount = 0;
            latestCumulativeTotal = 0;
        }
    }
}
