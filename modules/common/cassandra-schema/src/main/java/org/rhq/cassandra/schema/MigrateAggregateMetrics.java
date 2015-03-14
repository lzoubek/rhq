package org.rhq.cassandra.schema;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricsRegistry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.Days;

import org.rhq.cassandra.schema.migration.BatchResult;
import org.rhq.cassandra.schema.migration.FailedBatch;
import org.rhq.cassandra.schema.migration.QueryExecutor;
import org.rhq.core.util.exception.ThrowableUtil;

/**
 * <p>
 * Migrates aggregate metrics from the one_hour_metrics, six_hour_metrics, and twenty_four_hour_metrics tables to the
 * new aggregate_metrics table. The failure to migrate data for a single measurement schedule will result in an
 * exception being thrown that causes the upgrade to fail; however, all schedules will be processed even if there are
 * failures. An exception is thrown only after going through data for all schedules.
 * </p>
 * <p>
 * When data for a measurement schedule is successfully migrated, the schedule id is recorded in a log. There are
 * separate log files for each of the 1 hour, 6 hour, and 24 hour tables. They are stored in the server data directory.
 * Each table log is read prior to starting the migration to determine what schedule ids have data to be migrated.
 * </p>
 * <p>
 * After all data has been successfully migrated, the one_hour_metrics, six_hour_metrics, and twenty_four_hour_metrics
 * tables are dropped.
 * </p>
 *
 *
 * @author John Sanda
 */
public class MigrateAggregateMetrics implements Step {

    private static final Log log = LogFactory.getLog(MigrateAggregateMetrics.class);

    public static enum Bucket {

        ONE_HOUR("one_hour", "one_hour_metrics"),

        SIX_HOUR("six_hour", "six_hour_metrics"),

        TWENTY_FOUR_HOUR("twenty_four_hour", "twenty_four_hour_metrics");

        private String text;

        private String tableName;

        private Bucket(String text, String tableName) {
            this.text = text;
            this.tableName = tableName;
        }

        public String getTableName() {
            return tableName;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    public static final int DEFAULT_WARM_UP = 20;

    private static final double RATE_INCREASE_PER_NODE = 0.3;

    private Session session;

    private String dataDir;

    private DBConnectionFactory dbConnectionFactory;

    private AtomicReference<RateLimiter> readPermitsRef = new AtomicReference<RateLimiter>();

    private AtomicReference<RateLimiter> writePermitsRef = new AtomicReference<RateLimiter>();

    SchemaUpdateThreadFactory threadFactory = new SchemaUpdateThreadFactory();

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(6, threadFactory);

//    private ListeningExecutorService threadPool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(6,
//        new SchemaUpdateThreadFactory()));
    private ListeningExecutorService threadPool = MoreExecutors.listeningDecorator(executor);

    private MetricsRegistry metricsRegistry;

    private Meter migrationsMeter;

    private RateMonitor rateMonitor;

    private KeyScanner keyScanner;

    private MigrationProgressLogger progressLogger;

    private AtomicInteger remaining1HourMetrics = new AtomicInteger();

    private AtomicInteger remaining6HourMetrics = new AtomicInteger();

    private AtomicInteger remaining24HourMetrics = new AtomicInteger();

    private AtomicInteger readErrors = new AtomicInteger();

    private AtomicInteger writeErrors = new AtomicInteger();

    private QueryExecutor queryExecutor;

    public void setSession(Session session) {
        this.session = session;
    }

    @Override
    public void bind(Properties properties) {
        dbConnectionFactory = (DBConnectionFactory) properties.get(SchemaManager.RELATIONAL_DB_CONNECTION_FACTORY_PROP);
        dataDir = properties.getProperty("data.dir", System.getProperty("jboss.server.data.dir"));
    }

    @Override
    public void execute() {
        log.info("Starting data migration");
        if (System.getProperty("threadpool.fixed") != null) {
            log.info("Using fixed queue size executor");
            executor = new ThreadPoolExecutor(6, 6, 6, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100));
        }
        metricsRegistry = new MetricsRegistry();
        migrationsMeter = metricsRegistry.newMeter(MigrateAggregateMetrics.class, "migrations", "migrations",
            TimeUnit.MINUTES);

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            // dbConnectionFactory can be null in test environments which is fine because we start tests with a brand
            // new schema and cluster. In this case, we do not need to do anything since it is not an upgrade scenario.
            if (dbConnectionFactory == null) {
                log.info("The relational database connection factory is not set. No data migration necessary");
                return;
            }

            writePermitsRef.set(RateLimiter.create(getWriteLimit(getNumberOfUpNodes()), DEFAULT_WARM_UP,
                TimeUnit.SECONDS));
            readPermitsRef.set(RateLimiter.create(getReadLimit(getNumberOfUpNodes()), DEFAULT_WARM_UP,
                TimeUnit.SECONDS));

            log.info("The request limits are " + writePermitsRef.get().getRate() + " writes/sec and " +
                readPermitsRef.get().getRate() + " reads/sec");

            progressLogger = new MigrationProgressLogger();
            rateMonitor = new RateMonitor(readPermitsRef, writePermitsRef);
            if (System.getProperty("rate.monitor.2") != null) {
                log.info("Using rateMonitor2");
                rateMonitor = new RateMonitor2(readPermitsRef, writePermitsRef);
            }
            if (System.getProperty("rate.monitor.3") != null) {
                log.info("Using rateMonitor 3");
                rateMonitor = new RateMonitor3(readPermitsRef, writePermitsRef);
            }
            queryExecutor = new QueryExecutor(session, readPermitsRef, writePermitsRef);

            keyScanner = new KeyScanner(session);
            Set<Integer> scheduleIdsWith1HourData = Collections.emptySet();
            Set<Integer> scheduleIdsWith6HourData = Collections.emptySet();
            Set<Integer> scheduleIdsWith24HourData = Collections.emptySet();

            Set<String> tables = getTables();
            if (tables.contains(Bucket.ONE_HOUR.getTableName())) {
                scheduleIdsWith1HourData = keyScanner.scanFor1HourKeys();
                remaining1HourMetrics.set(scheduleIdsWith1HourData.size());
                log.info("There are " + scheduleIdsWith1HourData.size() + " schedule ids with " + Bucket.ONE_HOUR +
                    " data");
            } else {
                log.info(Bucket.ONE_HOUR + " data has already been migrated");
            }
            if (tables.contains(Bucket.SIX_HOUR.getTableName())) {
                scheduleIdsWith6HourData = keyScanner.scanFor6HourKeys();
                remaining6HourMetrics.set(scheduleIdsWith6HourData.size());
                log.info("There are " + scheduleIdsWith6HourData.size() + " schedule ids with " + Bucket.SIX_HOUR +
                    " data");
            } else {
                log.info(Bucket.SIX_HOUR + " data has already been migrated");
            }
            if (tables.contains(Bucket.TWENTY_FOUR_HOUR.getTableName())) {
                scheduleIdsWith24HourData = keyScanner.scanFor24HourKeys();
                remaining24HourMetrics.set(scheduleIdsWith24HourData.size());
                log.info("There are " + scheduleIdsWith24HourData.size() + " schedule ids with " +
                    Bucket.TWENTY_FOUR_HOUR + " data");
            } else {
                log.info(Bucket.TWENTY_FOUR_HOUR + " data has already been migrated");
            }
            keyScanner.shutdown();

            threadPool.submit(progressLogger);
            threadPool.submit(rateMonitor);

            if (!scheduleIdsWith1HourData.isEmpty()) {
                migrate1HourData(scheduleIdsWith1HourData);
            }
            if (!scheduleIdsWith6HourData.isEmpty()) {
                migrate6HourData(scheduleIdsWith6HourData);
            }
            if (!scheduleIdsWith24HourData.isEmpty()) {
                migrate24HourData(scheduleIdsWith24HourData);
            }

            stopwatch.stop();
            log.info("Finished migrating " + scheduleIdsWith1HourData.size() + " " + Bucket.ONE_HOUR + ", " +
                scheduleIdsWith6HourData.size() + " " + Bucket.SIX_HOUR + ", " + scheduleIdsWith24HourData.size() +
                " " + Bucket.TWENTY_FOUR_HOUR + " metrics in " + stopwatch.elapsed(TimeUnit.SECONDS) + " sec");
        } catch (IOException e) {
            throw new RuntimeException("There was an unexpected I/O error. There are unfinished metrics migrations " +
                "- " + getRemainingMetrics() + ". The upgrade will have to be run again.");
        } catch (AbortedException e) {
            throw new RuntimeException("The key scan was aborted. The upgrade will have to be rerun.", e);
        } catch (InterruptedException e) {
            throw new RuntimeException("The migration was interrupted. There are still " + getRemainingMetrics() +
                " unfinished metrics migrations. The upgrade will have to be run again.");
        } finally {
            shutdown();
        }
    }

    private String getRemainingMetrics() {
        return "{" + Bucket.ONE_HOUR + ": " + remaining1HourMetrics + ", " + Bucket.SIX_HOUR + ": " +
            remaining6HourMetrics + ", " + Bucket.TWENTY_FOUR_HOUR + ": " + remaining24HourMetrics + "}";
    }

    private double getWriteLimit(int numNodes) {
        int baseLimit = Integer.parseInt(System.getProperty("rhq.storage.request.write-limit", "10000"));
        double increase = baseLimit * RATE_INCREASE_PER_NODE;
        return baseLimit + (increase * (numNodes - 1));
    }

    private double getReadLimit(int numNodes) {
        int baseLimit = Integer.parseInt(System.getProperty("rhq.storage.request.read-limit", "25"));
        double increase = baseLimit * RATE_INCREASE_PER_NODE;
        return baseLimit + (increase * (numNodes - 1));
    }

    private int getNumberOfUpNodes() {
        int count = 0;
        for (Host host : session.getCluster().getMetadata().getAllHosts()) {
            if (host.isUp()) {
                ++count;
            }
        }
        return count;
    }

    private void shutdown() {
        try {
            log.info("Shutting down migration thread pools...");
            rateMonitor.shutdown();
            progressLogger.finished();
            keyScanner.shutdown();
            threadPool.shutdown();
            threadPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
    }

    private void migrate1HourData(Set<Integer> scheduleIds) throws IOException, InterruptedException {
        DateTime endTime = DateUtils.get1HourTimeSlice(DateTime.now());
        DateTime startTime = endTime.minus(Days.days(14));
        PreparedStatement query = session.prepare(
            "SELECT time, type, value FROM rhq.one_hour_metrics " +
            "WHERE schedule_id = ? AND time >= " + startTime.getMillis() + " AND time <= " + endTime.getMillis());
        migrateData(scheduleIds, query, Bucket.ONE_HOUR, Days.days(14), remaining1HourMetrics);
    }

    private void migrate6HourData(Set<Integer> scheduleIds) throws IOException, InterruptedException {
        DateTime endTime = DateUtils.get1HourTimeSlice(DateTime.now());
        DateTime startTime = endTime.minus(Days.days(31));
        PreparedStatement query = session.prepare(
            "SELECT time, type, value FROM rhq.six_hour_metrics " +
            "WHERE schedule_id = ? AND time >= " + startTime.getMillis() + " AND time <= " + endTime.getMillis());
        migrateData(scheduleIds, query, Bucket.SIX_HOUR, Days.days(31), remaining6HourMetrics);
    }

    private void migrate24HourData(Set<Integer> scheduleIds) throws IOException, InterruptedException {
        DateTime endTime = DateUtils.get1HourTimeSlice(DateTime.now());
        DateTime startTime = endTime.minus(Days.days(365));
        PreparedStatement query = session.prepare(
            "SELECT time, type, value FROM rhq.twenty_four_hour_metrics " +
            "WHERE schedule_id = ? AND time >= " + startTime.getMillis() + " AND time <= " + endTime.getMillis());
        migrateData(scheduleIds, query, Bucket.TWENTY_FOUR_HOUR, Days.days(365), remaining24HourMetrics);
    }

    private void migrateData(Set<Integer> scheduleIds, PreparedStatement query, Bucket bucket, Days ttl,
        AtomicInteger remainingMetrics) throws IOException, InterruptedException {

        Stopwatch stopwatch = Stopwatch.createStarted();
        log.info("Starting data migration for " + bucket + " data");
        MigrationLog migrationLog = new MigrationLog(new File(dataDir, bucket + "_migration.log"));
        try {
            Set<Integer> migratedScheduleIds = migrationLog.read();
            Set<FailedBatch> failedBatches = Collections.newSetFromMap(new ConcurrentHashMap<FailedBatch, Boolean>());

            while (!scheduleIds.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("Submitting migration tasks for " + scheduleIds.size() + " schedule ids - " +
                        scheduleIds);
                } else {
                    log.info("Submitting migration tasks for " + scheduleIds.size() + " schedule ids");
                }
                scheduleIds = submitMigrationTasks(scheduleIds, query, bucket, ttl, migrationLog, migratedScheduleIds,
                    failedBatches, remainingMetrics);
            }

            int totalFailedBatches = failedBatches.size();
            Stopwatch batchesStopwatch = Stopwatch.createStarted();
            while (!failedBatches.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("Retrying " + failedBatches.size() + " failed batches for " + bucket + " data - " +
                        failedBatches);
                } else {
                    log.info("Retrying " + failedBatches.size() + " failed batches for " + bucket + " data");
                }

                failedBatches = retryFailedBatches(bucket, failedBatches);
                if (log.isDebugEnabled() && !failedBatches.isEmpty()) {
                    log.debug("Retrying " + failedBatches.size() + " failed batches for " + bucket + " data");
                }
            }
            batchesStopwatch.stop();
            log.info("Successfully reinserted " + totalFailedBatches + " batches in " +
                batchesStopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
            try {
                dropTable(bucket);
            } catch (Exception e) {
                log.warn("DROP " + bucket + " failed", e);
                log.info("Tables are - " + getTables());
            }
            stopwatch.stop();
            log.info("Finished migrating " + bucket + " data in " + stopwatch.elapsed(TimeUnit.SECONDS) + " sec");
        } finally {
            migrationLog.close();
        }
    }

    private Set<FailedBatch> retryFailedBatches(Bucket bucket, Set<FailedBatch> failedBatches)
        throws InterruptedException {

        final Set<FailedBatch> remaining = Collections.newSetFromMap(new ConcurrentHashMap<FailedBatch, Boolean>());
        final CountDownLatch latch = new CountDownLatch(failedBatches.size());

        for (final FailedBatch failedBatch : failedBatches) {
            ResultSetFuture future = queryExecutor.executeWrite(failedBatch.getBatch());
            Futures.addCallback(future, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(ResultSet result) {
                    rateMonitor.requestSucceeded();
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    log.info("Batch write failed: " + ThrowableUtil.getRootMessage(t));
                    rateMonitor.requestFailed();
                    remaining.add(failedBatch);
                    writeErrors.incrementAndGet();
                    latch.countDown();
                }
            });
        }

        String errorMsg = bucket + " data migration appears to be blocked. There are " + latch.getCount() +
            " failed batches that have to be retried.";
        await(latch, errorMsg);

        return remaining;
    }

    private Set<Integer> submitMigrationTasks(Set<Integer> scheduleIds, PreparedStatement query, final Bucket bucket,
        Days ttl, final MigrationLog migrationLog, Set<Integer> migratedScheduleIds, final Set<FailedBatch> failedBatches,
        final AtomicInteger remainingMetrics)
        throws InterruptedException {

        final Set<Integer> failedScheduleIds = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        final CountDownLatch remaining = new CountDownLatch(scheduleIds.size());

        for (final Integer scheduleId : scheduleIds) {
            if (migratedScheduleIds.contains(scheduleId)) {
                if (log.isDebugEnabled()) {
                    log.debug(bucket + " data for schedule id " + scheduleId + " has already been migrated");
                }
                remainingMetrics.decrementAndGet();
                remaining.countDown();
            } else {
                ListenableFuture<ResultSet> queryFuture = queryExecutor.executeRead(query.bind(scheduleId));
//                queryFuture = Futures.withFallback(queryFuture, countReadErrors);
                ListenableFuture<List<BatchResult>> insertsFuture = Futures.transform(queryFuture,
                    new MigrateData(scheduleId, bucket, queryExecutor, ttl.toStandardSeconds(), writeErrors,
                        rateMonitor, failedBatches), threadPool);
                Futures.addCallback(insertsFuture, new FutureCallback<List<BatchResult>>() {
                    @Override
                    public void onSuccess(List<BatchResult> results) {
                        try {
                            boolean migrationFailed = false;
                            for (BatchResult result : results) {
                                if (result.succeeded()) {
                                    rateMonitor.requestSucceeded();
                                } else {
                                    migrationFailed = true;
                                    rateMonitor.requestFailed();
                                    writeErrors.incrementAndGet();
                                    failedBatches.add(new FailedBatch(scheduleId, result.getBatch()));
                                }
                            }

                            if (!migrationFailed) {
                                migrationsMeter.mark();
                                migrationLog.write(scheduleId);
                                remainingMetrics.decrementAndGet();
                            }

                            remaining.countDown();
                        } catch (IOException e) {
                            log.warn("Failed to log successful migration of " + bucket + " data for schedule id " +
                                scheduleId, e);
                        } catch (Exception e) {
                            log.error("The onSuccess callback failed. This is a bug!", e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        if (log.isDebugEnabled()) {
                            log.debug("Failed to fetch " + bucket + " data for schedule id " + scheduleId +
                                ". Migration will be retried.", t);
                        } else {
                            log.info("Failed to fetch " + bucket + " data for schedule id " + scheduleId + ": " +
                                ThrowableUtil.getRootMessage(t) + ". Migration will be retried.");
                        }
                        readErrors.incrementAndGet();
                        failedScheduleIds.add(scheduleId);
                        rateMonitor.requestFailed();
                        remaining.countDown();
                    }
                }, threadPool);
            }
        }
        String errorMsg = bucket + " data migration appears to be blocked. There are " + remaining.getCount() +
            " remaining migrations out of " + scheduleIds.size() + " with " + failedScheduleIds.size() +
            " that have failed. The migration will have to be restarted.";
        await(remaining, errorMsg);

        return failedScheduleIds;
    }

    private void await(CountDownLatch latch, String errorMsg) throws InterruptedException {
        int count = 0;
        long previousRemaining = latch.getCount();
        while (latch.getCount() > 0) {
            latch.await(1, TimeUnit.SECONDS);
            if (previousRemaining == latch.getCount()) {
                ++count;
            } else {
                previousRemaining = latch.getCount();
                count = 0;
            }
            if (count == 30) {
                throw new RuntimeException(errorMsg);
            }
        }
    }

//    private ListenableFuture<List<ResultSet>> migrateData(PreparedStatement query, Bucket bucket, Integer scheduleId,
//        Days ttl, Set<FailedBatch> failedBatches) {
//
//        ListenableFuture<ResultSet> queryFuture = queryExecutor.executeRead(query.bind(scheduleId));
//        queryFuture = Futures.withFallback(queryFuture, countReadErrors);
//        return  Futures.transform(queryFuture,
//            new MigrateData(scheduleId, bucket, queryExecutor, ttl.toStandardSeconds(), writeErrors, rateMonitor,
//                failedBatches), threadPool);
//        return migrationFuture;
//
////        try {
////            ListenableFuture<ResultSet> queryFuture = queryExecutor.executeRead(query.bind(scheduleId));
////            queryFuture = Futures.withFallback(queryFuture, countReadErrors);
////            ListenableFuture<List<ResultSet>> migrationFuture = Futures.transform(queryFuture,
////                new MigrateData(scheduleId, bucket, queryExecutor, ttl.toStandardSeconds(), writeErrors, rateMonitor,
////                    failedBatches), threadPool);
////            return migrationFuture;
////        } catch (Exception e) {
////            log.warn("FAILED to submit " + bucket + " data migration tasks for schedule id " + scheduleId, e);
////        }
////    }

//    private FutureCallback<List<ResultSet>> migrationFinished(final PreparedStatement query,
//        final PreparedStatement delete, final Integer scheduleId, final Bucket bucket, final MigrationLog migrationLog,
//        final CountDownLatch remainingMetrics, final Days ttl, final Set<FailedBatch> failedBatches) {
//
//        return new FutureCallback<List<ResultSet>>() {
//            @Override
//            public void onSuccess(List<ResultSet> batchResults) {
//                try {
//                    remainingMetrics.countDown();
//                    migrationLog.write(scheduleId);
//                    rateMonitor.requestSucceeded();
//                    migrationsMeter.mark();
//                    if (log.isDebugEnabled()) {
//                        log.debug("Finished migrating " + bucket + " data for schedule id " + scheduleId);
//                    }
//                } catch (IOException e) {
//                    log.warn("Failed to log successful migration of " + bucket + " data for schedule id " +
//                        scheduleId, e);
//                } catch (Exception e) {
//                    log.error("The migration finished callback failed. This should not happen. Please file a " +
//                        "bug report.", e);
//                }
//            }
//
//            @Override
//            public void onFailure(Throwable t) {
//                try {
//                    if (t instanceof ReadException) {
//                        if (log.isDebugEnabled()) {
//                            log.debug("Failed to fetch " + bucket + " data for schedule id " + scheduleId +
//                                ". Migration will be retried.", t);
//                        } else {
//                            log.info("Failed to fetch " + bucket + " data for schedule id " + scheduleId + ": " +
//                                ThrowableUtil.getRootMessage(t) + ". Migration will be retried.");
//                        }
//                        migrateData(query, delete, bucket, remainingMetrics, migrationLog, scheduleId, ttl,
//                            failedBatches);
//                    } else {
//                        if (log.isDebugEnabled()) {
//                            log.debug("One or more writes of " + bucket + " data for schedule id " + scheduleId +
//                                "failed. These will be retried.", t);
//                        } else {
//                            log.info("One or more writes of " + bucket + " data for schedule id " + scheduleId +
//                                "failed: " + ThrowableUtil.getRootMessage(t) + ". These writes will be retried");
//                        }
//                        remainingMetrics.countDown();
//                    }
//                } catch (Exception e) {
//                    log.error("The migration finished callback failed. This should not happen. Please file a bug " +
//                        "report.", e);
//                }
//            }
//        };
//    }

    private class MigrationProgressLogger implements Runnable {

        private boolean finished;

        private boolean reportMigrationRates;

        public void finished() {
            finished = true;
        }

        @Override
        public void run() {
            try {
                while (!finished) {
                    log.info("Remaining metrics to migrate\n" +
                        Bucket.ONE_HOUR + ": " + remaining1HourMetrics + "\n" +
                        Bucket.SIX_HOUR + ": " + remaining6HourMetrics + "\n" +
                        Bucket.TWENTY_FOUR_HOUR + ": " + remaining24HourMetrics + "\n");
                    log.info("ErrorCounts{read:" + readErrors + ", write: " + writeErrors + "}");
                    if (reportMigrationRates) {
                        log.info("Metrics migration rates:\n" +
                            "1 min rate: "  + migrationsMeter.oneMinuteRate() + "\n" +
                            "5 min rate: " + migrationsMeter.fiveMinuteRate() + " \n" +
                            "15 min rate: " + migrationsMeter.fifteenMinuteRate() + "\n");
                        reportMigrationRates = false;
                    } else {
                        reportMigrationRates = true;
                    }

                    Thread.sleep(30000);
                }
            } catch (InterruptedException e) {
            }
        }
    }

    private void dropTable(Bucket bucket) {
        log.info("Dropping table " + bucket.getTableName());
        session.execute("DROP table rhq." + bucket.getTableName());
    }

    private Set<String> getTables() {
        ResultSet resultSet = session.execute("SELECT columnfamily_name FROM system.schema_columnfamilies " +
            "WHERE keyspace_name = 'rhq'");
        Set<String> tables = new HashSet<String>();

        for (Row row : resultSet) {
            tables.add(row.getString(0));
        }

        return tables;
    }

}
