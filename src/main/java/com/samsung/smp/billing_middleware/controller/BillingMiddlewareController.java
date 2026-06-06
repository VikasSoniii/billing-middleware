package com.samsung.smp.billing_middleware.controller;

import com.samsung.smp.billing_middleware.service.ExactSampleCounterService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1")
@CrossOrigin(origins = "*")
public class BillingMiddlewareController {

    @Autowired
    private ExactSampleCounterService counterService;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${cortex.distributor.url:http://localhost:9091}")
    private String cortexUrl;

    @PostMapping("/push")
    public ResponseEntity<byte[]> handleRemoteWrite(
            @RequestHeader(value = "X-Scope-OrgID", required = false) String tenantId,
            @RequestHeader(value = "Content-Encoding", required = false) String contentEncoding,
            HttpServletRequest request) {

        log.info("------------Inside handleRemoteWrite()---------------");
        long startTime = System.currentTimeMillis();
        log.info("Tenant Id: {}, Content-Encoding: {}", tenantId, contentEncoding);

        if (tenantId == null || tenantId.isEmpty()) {
            tenantId = "unknown";
            log.warn("No tenant ID provided, using: {}", tenantId);
        }

        try {
            // Read request body
            byte[] requestBody = readRequestBody(request);
            log.info("Received body size: {} bytes", requestBody.length);

            // DECOMPRESS SNAPPY DATA
            byte[] decompressedBody;
            if ("snappy".equalsIgnoreCase(contentEncoding)) {
                log.info("Decompressing snappy data...");
                decompressedBody = Snappy.uncompress(requestBody);
                log.info("Decompressed from {} bytes to {} bytes",
                        requestBody.length, decompressedBody.length);
            } else {
                log.info("No compression detected, using raw data");
                decompressedBody = requestBody;
            }

            // 1. COUNT EXACT SAMPLES (using decompressed data)
            long exactSampleCount = counterService.countExactSamples(tenantId, decompressedBody);

            // 2. FORWARD TO CORTEX
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Scope-OrgID", tenantId);
            headers.set("Content-Type", "application/x-protobuf");

            // Forward original compressed data to Cortex
            org.springframework.http.HttpEntity<byte[]> entity =
                    new org.springframework.http.HttpEntity<>(requestBody, headers);

            /*ResponseEntity<byte[]> cortexResponse = null;
            try {
                cortexResponse = restTemplate.postForEntity(cortexUrl, entity, byte[].class);
                log.info("Cortex response: {}", cortexResponse.getStatusCode());
            } catch (Exception e) {
                log.warn("Failed to forward to Cortex: {}", e.getMessage());
            }

            long duration = System.currentTimeMillis() - startTime;
            log.info("Tenant: {} - Exact samples: {} - Duration: {}ms",
                    tenantId, exactSampleCount, duration);*/

            // 3. RETURN RESPONSE
            /*if (cortexResponse != null && cortexResponse.getStatusCode().is2xxSuccessful()) {
                return ResponseEntity.status(cortexResponse.getStatusCode()).build();
            } else {
                return ResponseEntity.ok().build();
            }*/
            return ResponseEntity.ok().build();
        } catch (IOException e) {
            log.error("Failed to read/decompress request body for tenant: {}", tenantId, e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        } catch (Exception e) {
            log.error("Failed to process request for tenant: {}", tenantId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    private byte[] readRequestBody(HttpServletRequest request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = request.getInputStream().read(buffer)) != -1) {
            baos.write(buffer, 0, bytesRead);
        }
        return baos.toByteArray();
    }

    @GetMapping("/debug/samples/{tenantId}")
    public Map<String, Object> getTenantSamples(@PathVariable String tenantId) {
        Map<String, Object> response = new HashMap<>();
        response.put("tenantId", tenantId);
        response.put("exactSampleCount", counterService.getExactSampleCount(tenantId));
        response.put("batchSizes", counterService.getBatchSizes());
        return response;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        Map<String, String> status = new HashMap<>();
        status.put("status", "UP");
        status.put("service", "exact-sample-counter-middleware");
        return status;
    }
}