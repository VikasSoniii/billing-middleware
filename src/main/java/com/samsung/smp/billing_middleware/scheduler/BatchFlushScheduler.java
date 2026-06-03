package com.samsung.smp.billing_middleware.scheduler;

import com.samsung.smp.billing_middleware.service.ExactSampleCounterService;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BatchFlushScheduler {
    @Autowired
    private ExactSampleCounterService counterService;

    @Scheduled(fixedRateString = "${billing.flush.interval:60000}")
    public void flushBatches() {
        log.debug("Flushing sample batches to Kafka");
        counterService.flushBatchesToKafka();
    }
}
