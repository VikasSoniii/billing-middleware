package com.samsung.smp.billing_middleware.repository;

import com.samsung.smp.billing_middleware.entity.ExactSampleRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface ExactSampleRepository extends JpaRepository<ExactSampleRecord, Long> {

    @Query("SELECT SUM(e.sampleCount) FROM ExactSampleRecord e " +
            "WHERE e.tenantId = :tenantId AND e.windowStart >= :startDate")
    Long getTotalSamplesSince(@Param("tenantId") String tenantId,
                              @Param("startDate") LocalDateTime startDate);

    @Query("SELECT e FROM ExactSampleRecord e " +
            "WHERE e.tenantId = :tenantId " +
            "AND e.windowStart BETWEEN :start AND :end " +
            "ORDER BY e.windowStart ASC")
    List<ExactSampleRecord> getRecordsForPeriod(@Param("tenantId") String tenantId,
                                                @Param("start") LocalDateTime start,
                                                @Param("end") LocalDateTime end);
}