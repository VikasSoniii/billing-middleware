package com.samsung.smp.billing_middleware.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BillingMessage {
    private String tenantId;
    private long samplesCount;
    private int requestCount;
    private long cumulativeTotal;
    private Instant timestamp;
    private LocalDateTime timeWindowStart;
    private LocalDateTime timeWindowEnd;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public long getSamplesCount() {
        return samplesCount;
    }

    public void setSamplesCount(long samplesCount) {
        this.samplesCount = samplesCount;
    }

    public int getRequestCount() {
        return requestCount;
    }

    public void setRequestCount(int requestCount) {
        this.requestCount = requestCount;
    }

    public long getCumulativeTotal() {
        return cumulativeTotal;
    }

    public void setCumulativeTotal(long cumulativeTotal) {
        this.cumulativeTotal = cumulativeTotal;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public LocalDateTime getTimeWindowStart() {
        return timeWindowStart;
    }

    public void setTimeWindowStart(LocalDateTime timeWindowStart) {
        this.timeWindowStart = timeWindowStart;
    }

    public LocalDateTime getTimeWindowEnd() {
        return timeWindowEnd;
    }

    public void setTimeWindowEnd(LocalDateTime timeWindowEnd) {
        this.timeWindowEnd = timeWindowEnd;
    }
}