package com.samsung.smp.billing_middleware.controller;

import com.samsung.smp.billing_middleware.service.DeadLetterQueueService;
import com.samsung.smp.billing_middleware.service.ExactSampleCounterService;
import com.samsung.smp.billing_middleware.validation.RequestValidator;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/v1")
@CrossOrigin(origins = "*")
public class BillingMiddlewareController {

    @Autowired
    private ExactSampleCounterService counterService;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private DeadLetterQueueService deadLetterQueueService;

    @Autowired
    private RetryTemplate retryTemplate;

    @Autowired
    private RequestValidator requestValidator;

    @Autowired
    private MeterRegistry meterRegistry;

    @Value("${cortex.distributor.url:http://localhost:9009/api/v1/push}")
    private String cortexUrl;

    @Value("${cortex.forward.enabled:true}")
    private boolean cortexForwardEnabled;

    /**
     * PRODUCTION-GRADE Remote Write Handler
     * - Request validation
     * - Snappy decompression
     * - Sample counting (only if Cortex accepts)
     * - Retry with circuit breaker
     * - Dead Letter Queue for failures
     */
    @PostMapping("/push")
    public ResponseEntity<Map<String, Object>> handleRemoteWrite(
            @RequestHeader(value = "X-Scope-OrgID", required = false) String tenantId,
            @RequestHeader(value = "Content-Encoding", required = false) String contentEncoding,
            @RequestHeader(value = "X-API-Key", required = false) String apiKey,
            HttpServletRequest request) {

        String requestId = UUID.randomUUID().toString().substring(0, 8);
        long startTime = System.currentTimeMillis();

        log.info("[{}] Inside handleRemoteWrite() - Tenant: {}, Content-Encoding: {}",
                requestId, tenantId, contentEncoding);

        try {
            // ==========================================
            // 1. VALIDATE REQUEST
            // ==========================================
            if (tenantId == null || tenantId.isEmpty()) {
                tenantId = "unknown";
                log.warn("[{}] No tenant ID provided", requestId);
            }

            // ==========================================
            // 2. READ REQUEST BODY
            // ==========================================
            byte[] requestBody = readRequestBody(request);
            log.info("[{}] Received body size: {} bytes", requestId, requestBody.length);

            // Validate request size
            if (requestBody.length > 10 * 1024 * 1024) { // 10MB limit
                log.warn("[{}] Request too large: {} bytes", requestId, requestBody.length);
                return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE)
                        .body(Map.of("error", "Request too large", "requestId", requestId));
            }

            // ==========================================
            // 3. DECOMPRESS SNAPPY DATA
            // ==========================================
            byte[] decompressedBody;
            if ("snappy".equalsIgnoreCase(contentEncoding)) {
                decompressedBody = Snappy.uncompress(requestBody);
                log.info("[{}] Decompressed: {} -> {} bytes",
                        requestId, requestBody.length, decompressedBody.length);
            } else {
                decompressedBody = requestBody;
            }

            // ==========================================
            // 4. FORWARD TO CORTEX FIRST (with retry)
            // ==========================================
            boolean cortexSuccess = true;
            if (cortexForwardEnabled) {
                cortexSuccess = forwardToCortexWithRetry(tenantId, requestBody, requestId);

                if (!cortexSuccess) {
                    // Store in Dead Letter Queue for later replay
                    deadLetterQueueService.storeFailedMessage(
                            tenantId, requestBody, "Cortex forwarding failed");

                    log.error("[{}] Cortex forwarding failed after retries", requestId);
                    meterRegistry.counter("billing.cortex.failure",
                            "tenant", tenantId).increment();

                    return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                            .body(Map.of(
                                    "error", "Failed to forward to storage",
                                    "requestId", requestId,
                                    "message", "Data stored in DLQ for replay"
                            ));
                }
            }

            // ==========================================
            // 5. COUNT SAMPLES (only after Cortex accepts)
            // ==========================================
            long exactSampleCount = counterService.countExactSamples(tenantId, decompressedBody);

            long duration = System.currentTimeMillis() - startTime;
            log.info("[{}] SUCCESS - Tenant: {}, Samples: {}, Duration: {}ms",
                    requestId, tenantId, exactSampleCount, duration);

            // ==========================================
            // 6. RETURN SUCCESS RESPONSE
            // ==========================================
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("samplesCounted", exactSampleCount);
            response.put("durationMs", duration);
            response.put("requestId", requestId);
            response.put("timestamp", Instant.now().toString());

            return ResponseEntity.ok(response);

        } catch (IOException e) {
            log.error("[{}] IO Error: {}", requestId, e.getMessage());
            meterRegistry.counter("billing.errors", "type", "io_error").increment();

            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Failed to process request body",
                            "requestId", requestId));

        } catch (Exception e) {
            log.error("[{}] Unexpected error: {}", requestId, e.getMessage(), e);
            meterRegistry.counter("billing.errors", "type", "unexpected").increment();

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Internal processing error",
                            "requestId", requestId));
        }
    }

    /**
     * Forward to Cortex with Retry Logic
     */
    private boolean forwardToCortexWithRetry(String tenantId, byte[] requestBody, String requestId) {
        try {
            return retryTemplate.execute(context -> {
                int attempt = context.getRetryCount() + 1;
                log.info("[{}] Forwarding to Cortex - Attempt {}/3", requestId, attempt);

                boolean success = forwardToCortex(tenantId, requestBody);

                if (success) {
                    log.info("[{}] Cortex forwarding successful on attempt {}", requestId, attempt);
                    meterRegistry.counter("billing.cortex.success").increment();
                    return true;
                } else {
                    log.warn("[{}] Cortex forwarding failed on attempt {}", requestId, attempt);
                    throw new RuntimeException("Cortex returned non-200");
                }
            });
        } catch (Exception e) {
            log.error("[{}] All retry attempts exhausted", requestId, e);
            meterRegistry.counter("billing.cortex.retries.exhausted").increment();
            return false;
        }
    }

    /**
     * Forward to Cortex with Circuit Breaker
     */
    @CircuitBreaker(name = "cortex", fallbackMethod = "handleCortexFallback")
    private boolean forwardToCortex(String tenantId, byte[] requestBody) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Scope-OrgID", tenantId);
            headers.set("Content-Type", "application/x-protobuf");
            headers.set("Content-Encoding", "snappy");
            headers.set("X-Request-ID", UUID.randomUUID().toString());

            org.springframework.http.HttpEntity<byte[]> entity =
                    new org.springframework.http.HttpEntity<>(requestBody, headers);

            ResponseEntity<byte[]> cortexResponse = restTemplate.postForEntity(
                    cortexUrl, entity, byte[].class);

            if (cortexResponse.getStatusCode().is2xxSuccessful()) {
                log.debug("Cortex accepted data for tenant: {}", tenantId);
                return true;
            } else {
                log.warn("Cortex returned non-200: {} for tenant: {}",
                        cortexResponse.getStatusCode(), tenantId);
                meterRegistry.counter("billing.cortex.error",
                        "status", cortexResponse.getStatusCode().toString()).increment();
                throw new RuntimeException("Cortex returned: " + cortexResponse.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Failed to forward to Cortex: {}", e.getMessage());
            meterRegistry.counter("billing.cortex.exception").increment();
            throw new RuntimeException("Cortex unavailable", e);
        }
    }

    /**
     * Fallback when Circuit Breaker is OPEN
     */
    private boolean handleCortexFallback(String tenantId, byte[] requestBody, Exception e) {
        log.error("Circuit breaker OPEN for Cortex. Error: {}", e.getMessage());
        meterRegistry.counter("billing.cortex.circuit.open").increment();

        // Store in DLQ
        deadLetterQueueService.storeFailedMessage(
                tenantId, requestBody, "Circuit breaker open: " + e.getMessage());

        return false;
    }

    /**
     * Read request body
     */
    private byte[] readRequestBody(HttpServletRequest request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = request.getInputStream().read(buffer)) != -1) {
            baos.write(buffer, 0, bytesRead);
        }
        return baos.toByteArray();
    }

    // ============================================
    // MONITORING ENDPOINTS
    // ============================================

    /**
     * Multi-tenant dashboard
     */
    @GetMapping("/monitoring/dashboard")
    public ResponseEntity<Map<String, Object>> getDashboard() {
        Map<String, Object> dashboard = new HashMap<>();

        List<Map<String, Object>> tenants = new ArrayList<>();
        String[] tenantIds = {"tenant-a", "tenant-b", "test-tenant-local"};

        for (String tenantId : tenantIds) {
            long samples = counterService.getExactSampleCount(tenantId);
            if (samples > 0) {
                Map<String, Object> t = new HashMap<>();
                t.put("tenantId", tenantId);
                t.put("samples", samples);
                t.put("batchSize", counterService.getBatchSizes().getOrDefault(tenantId, 0));
                tenants.add(t);
            }
        }

        tenants.sort((a, b) -> Long.compare(
                (Long) b.get("samples"), (Long) a.get("samples")));

        dashboard.put("activeTenants", tenants.size());
        dashboard.put("tenants", tenants);
        dashboard.put("timestamp", Instant.now().toString());

        long total = tenants.stream().mapToLong(t -> (Long) t.get("samples")).sum();
        dashboard.put("totalSamplesAllTenants", total);

        return ResponseEntity.ok(dashboard);
    }

    /**
     * Tenant sample details
     */
    @GetMapping("/debug/samples/{tenantId}")
    public Map<String, Object> getTenantSamples(@PathVariable String tenantId) {
        Map<String, Object> response = new HashMap<>();
        response.put("tenantId", tenantId);
        response.put("exactSampleCount", counterService.getExactSampleCount(tenantId));
        response.put("batchSizes", counterService.getBatchSizes());
        response.put("timestamp", Instant.now().toString());
        return response;
    }

    /**
     * Health check
     */
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "UP");
        status.put("service", "billing-middleware");
        status.put("version", "2.0.0-production");
        status.put("timestamp", Instant.now().toString());
        return status;
    }

    /**
     * Dead Letter Queue - View failed messages
     */
    @GetMapping("/admin/dlq/{tenantId}")
    public ResponseEntity<List<Map<String, Object>>> getDLQ(@PathVariable String tenantId) {
        return ResponseEntity.ok(deadLetterQueueService.getFailedMessages(tenantId));
    }

    /**
     * Dead Letter Queue - Replay failed messages
     */
    @PostMapping("/admin/dlq/{tenantId}/replay")
    public ResponseEntity<Map<String, Object>> replayDLQ(@PathVariable String tenantId) {
        int count = deadLetterQueueService.replayFailedMessages(tenantId);
        return ResponseEntity.ok(Map.of(
                "replayed", count,
                "tenantId", tenantId,
                "timestamp", Instant.now().toString()
        ));
    }

    /**
     * DLQ Statistics
     */
    @GetMapping("/admin/dlq/stats")
    public ResponseEntity<Map<String, Object>> getDLQStats() {
        return ResponseEntity.ok(deadLetterQueueService.getDLQStats());
    }

    /**
     * Metrics summary
     */
    @GetMapping("/metrics/summary")
    public ResponseEntity<Map<String, Object>> metricsSummary() {
        Map<String, Object> summary = new HashMap<>();

        try {
            summary.put("uptime", meterRegistry.get("process.uptime").gauge().value());
            summary.put("totalSamples", meterRegistry.get("billing.samples.total").counter().count());
            summary.put("totalRequests", meterRegistry.get("billing.requests.total").counter().count());
            summary.put("errors", meterRegistry.get("billing.errors.total").counter().count());
        } catch (Exception e) {
            log.warn("Some metrics not available yet");
        }

        summary.put("dlqStats", deadLetterQueueService.getDLQStats());
        summary.put("timestamp", Instant.now().toString());

        return ResponseEntity.ok(summary);
    }
}