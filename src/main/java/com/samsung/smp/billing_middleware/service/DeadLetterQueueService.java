package com.samsung.smp.billing_middleware.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

@Slf4j
@Service
public class DeadLetterQueueService {

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String DLQ_PREFIX = "dlq:";
    private static final int MAX_DLQ_SIZE = 1000;

    /**
     * Store failed message for later replay
     */
    public void storeFailedMessage(String tenantId, byte[] requestBody, String reason) {
        try {
            Map<String, Object> dlqEntry = new HashMap<>();
            dlqEntry.put("tenantId", tenantId);
            dlqEntry.put("data", Base64.getEncoder().encodeToString(requestBody));
            dlqEntry.put("reason", reason);
            dlqEntry.put("timestamp", Instant.now().toString());
            dlqEntry.put("size", requestBody.length);

            String dlqKey = DLQ_PREFIX + tenantId + ":" + UUID.randomUUID().toString();
            String value = objectMapper.writeValueAsString(dlqEntry);

            redisTemplate.opsForValue().set(dlqKey, value);
            redisTemplate.expire(dlqKey, 7, java.util.concurrent.TimeUnit.DAYS);

            // Add to failed list
            redisTemplate.opsForList().leftPush(DLQ_PREFIX + "failed:" + tenantId, dlqKey);

            log.info("Stored failed message in DLQ for tenant: {} (Reason: {})", tenantId, reason);

        } catch (Exception e) {
            log.error("Failed to store message in DLQ", e);
        }
    }

    /**
     * Get all failed messages for a tenant
     */
    public List<Map<String, Object>> getFailedMessages(String tenantId) {
        List<Map<String, Object>> messages = new ArrayList<>();

        List<String> keys = redisTemplate.opsForList()
                .range(DLQ_PREFIX + "failed:" + tenantId, 0, MAX_DLQ_SIZE);

        if (keys != null) {
            for (String key : keys) {
                String value = redisTemplate.opsForValue().get(key);
                if (value != null) {
                    try {
                        messages.add(objectMapper.readValue(value, Map.class));
                    } catch (Exception e) {
                        log.error("Failed to parse DLQ entry: {}", key, e);
                    }
                }
            }
        }

        return messages;
    }

    /**
     * Replay failed messages
     */
    public int replayFailedMessages(String tenantId) {
        int replayed = 0;
        List<String> keys = redisTemplate.opsForList()
                .range(DLQ_PREFIX + "failed:" + tenantId, 0, MAX_DLQ_SIZE);

        if (keys != null) {
            for (String key : keys) {
                String value = redisTemplate.opsForValue().get(key);
                if (value != null) {
                    try {
                        Map<String, Object> entry = objectMapper.readValue(value, Map.class);
                        String base64Data = (String) entry.get("data");
                        byte[] data = Base64.getDecoder().decode(base64Data);

                        // Re-process the message
                        // (You would call your controller/service here)
                        replayed++;

                        // Remove from DLQ after successful replay
                        redisTemplate.delete(key);
                        redisTemplate.opsForList().remove(DLQ_PREFIX + "failed:" + tenantId, 0, key);

                    } catch (Exception e) {
                        log.error("Failed to replay DLQ entry: {}", key, e);
                    }
                }
            }
        }

        log.info("Replayed {} failed messages for tenant: {}", replayed, tenantId);
        return replayed;
    }

    /**
     * Get DLQ statistics
     */
    public Map<String, Object> getDLQStats() {
        Map<String, Object> stats = new HashMap<>();

        Set<String> keys = redisTemplate.keys(DLQ_PREFIX + "failed:*");
        int totalFailed = 0;

        if (keys != null) {
            for (String key : keys) {
                Long size = redisTemplate.opsForList().size(key);
                if (size != null) {
                    totalFailed += size;
                }
            }
        }

        stats.put("totalFailedMessages", totalFailed);
        stats.put("failedTenants", keys != null ? keys.size() : 0);

        return stats;
    }
}