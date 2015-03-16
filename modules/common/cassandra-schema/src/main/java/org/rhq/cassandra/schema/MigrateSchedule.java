package org.rhq.cassandra.schema;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.RateLimiter;
import com.yammer.metrics.core.Meter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.Days;

import org.rhq.cassandra.schema.MigrateAggregateMetrics.Bucket;

public class MigrateSchedule implements Runnable {

    private static final Log log = LogFactory.getLog(MigrateSchedule.class);

    private final Session session;
    private final AtomicInteger readErrors;
    private final AtomicInteger writeErrors;
    private final Integer scheduleId;
    private final MigrateAggregateMetrics.Bucket bucket;
    private final PreparedStatement query;
    private final RateMonitor rateMonitor;
    private final Set<Integer> scheduleIds;
    private final Set<Integer> failedIds;
    private final MigrationLog migrationLog;
    private final AtomicReference<RateLimiter> writePermitsRef;
    private final AtomicReference<RateLimiter> readPermitsRef;
    private final Meter migrationsMeter;
    private final Days ttl;
    private final CountDownLatch migrations;
    private final AtomicInteger remainingMetrics;

    public MigrateSchedule(Session session, AtomicInteger readErrors, AtomicInteger writeErrors, 
        Integer scheduleId, Bucket bucket, PreparedStatement query, Set<Integer> scheduleIds, RateMonitor rateMonitor,
        MigrationLog migrationLog, AtomicReference<RateLimiter> writePermitsRef,
        AtomicReference<RateLimiter> readPermitsRef, Meter migrationsMeter, Days ttl, Set<Integer> failedIds,
        CountDownLatch migrations, AtomicInteger remainingMetrics)
 {
        this.session = session;
        this.readErrors = readErrors;
        this.writeErrors = writeErrors;
        this.scheduleId = scheduleId;
        this.scheduleIds = scheduleIds;
        this.bucket = bucket;
        this.query = query;
        this.rateMonitor = rateMonitor;
        this.migrationLog = migrationLog;
        this.readPermitsRef = readPermitsRef;
        this.writePermitsRef = writePermitsRef;
        this.migrationsMeter = migrationsMeter;
        this.ttl = ttl;
        this.failedIds = failedIds;
        this.migrations = migrations;
        this.remainingMetrics = remainingMetrics;
    }


    @Override
    public void run() {
        try {
            ResultSet readSet = null;
            try {
                readPermitsRef.get().acquire();
                readSet = session.execute(query.bind(scheduleId));
                rateMonitor.requestSucceeded();
            } catch (Throwable t) {
                readErrors.incrementAndGet();
                failedIds.add(scheduleId);
                rateMonitor.requestFailed();
                log.warn("Failed to read scheduleId=" + scheduleId);
                return;
            }

            try {
                MigrateData md = new MigrateData(scheduleId, bucket, writePermitsRef.get(), session,
                    ttl.toStandardSeconds());
                md.apply(readSet);
                remainingMetrics.decrementAndGet();
                migrations.countDown();
                scheduleIds.remove(scheduleId);
                rateMonitor.requestSucceeded();
                migrationsMeter.mark();
                migrationLog.write(scheduleId);

            } catch (Throwable ex) {
                failedIds.add(scheduleId);
                rateMonitor.requestFailed();
                log.warn("Failed to write batch for scheduleId=" + scheduleId);
            }

        } catch (Throwable t) {
            log.error("Failed to migrate metric", t);
        }
        

    }

}
