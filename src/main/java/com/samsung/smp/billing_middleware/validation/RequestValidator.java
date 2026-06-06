package com.samsung.smp.billing_middleware.validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Add class for Request Validation & Sanitization
 */
@Slf4j
@Component
public class RequestValidator {

    private static final int MAX_REQUEST_SIZE = 10 * 1024 * 1024; // 10MB
    private static final long MAX_TIMESTAMP_FUTURE_MS = 3600000; // 1 hour
    private static final long MAX_TIMESTAMP_PAST_MS = 86400000; // 24 hours

    public ValidationResult validate(byte[] requestBody, String tenantId) {
        ValidationResult result = new ValidationResult();

        // Size validation
        if (requestBody.length > MAX_REQUEST_SIZE) {
            result.addError("Request too large: " + requestBody.length + " bytes");
            return result;
        }

        if (requestBody.length == 0) {
            result.addError("Empty request body");
            return result;
        }

        // Tenant validation
        if (tenantId == null || tenantId.trim().isEmpty()) {
            result.addError("Missing tenant ID");
            return result;
        }

        if (tenantId.length() > 256) {
            result.addError("Tenant ID too long");
            return result;
        }

        // Sanitize tenant ID
        result.setSanitizedTenantId(sanitizeTenantId(tenantId));

        result.setValid(true);
        return result;
    }

    public boolean isValidTimestamp(long timestampMs) {
        long now = System.currentTimeMillis();
        long min = now - MAX_TIMESTAMP_PAST_MS;
        long max = now + MAX_TIMESTAMP_FUTURE_MS;

        if (timestampMs < min) {
            log.warn("Timestamp too old: {} (min: {})", Instant.ofEpochMilli(timestampMs), Instant.ofEpochMilli(min));
            return false;
        }

        if (timestampMs > max) {
            log.warn("Timestamp too far in future: {} (max: {})", Instant.ofEpochMilli(timestampMs), Instant.ofEpochMilli(max));
            return false;
        }

        return true;
    }

    private String sanitizeTenantId(String tenantId) {
        // Remove special characters, limit length
        return tenantId.replaceAll("[^a-zA-Z0-9_-]", "").substring(0, Math.min(tenantId.length(), 100));
    }

    public static class ValidationResult {
        private boolean valid = false;
        private String sanitizedTenantId;
        private final StringBuilder errors = new StringBuilder();

        public void addError(String error) {
            if (errors.length() > 0) errors.append("; ");
            errors.append(error);
        }

        public boolean isValid() { return valid; }
        public void setValid(boolean valid) { this.valid = valid; }

        public String getSanitizedTenantId() { return sanitizedTenantId; }
        public void setSanitizedTenantId(String id) { this.sanitizedTenantId = id; }

        public String getErrors() { return errors.toString(); }
    }
}