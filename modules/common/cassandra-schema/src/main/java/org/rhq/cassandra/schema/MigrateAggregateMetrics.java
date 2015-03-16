package org.rhq.cassandra.schema;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricsRegistry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.Days;

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

        ONE_HOUR("one_hour"),

        SIX_HOUR("six_hour"),

        TWENTY_FOUR_HOUR("twenty_four_hour");

        private String tableName;

        private Bucket(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public String toString() {
            return tableName;
        }
    }

    public static final int DEFAULT_WARM_UP = 20;

    private static final double RATE_INCREASE_PER_NODE = 0.3;

    private Session session;

    private String dataDir;

    private DBConnectionFactory dbConnectionFactory;

    private AtomicReference<RateLimiter> readPermitsRef = new AtomicReference<RateLimiter>();

    private AtomicReference<RateLimiter> writePermitsRef = new AtomicReference<RateLimiter>();

    private AtomicInteger remaining1HourMetrics = new AtomicInteger();

    private AtomicInteger remaining6HourMetrics = new AtomicInteger();

    private AtomicInteger remaining24HourMetrics = new AtomicInteger();

    private AtomicInteger migrated1HourMetrics = new AtomicInteger();

    private AtomicInteger migrated6HourMetrics = new AtomicInteger();

    private AtomicInteger migrated24HourMetrics = new AtomicInteger();

    private ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(100);

    private MetricsRegistry metricsRegistry;

    private Meter migrationsMeter;

    private CountDownLatch migrations;

    private RateMonitor rateMonitor;

    private KeyScanner keyScanner;

    private MigrationProgressLogger progressLogger;

    private AtomicInteger readErrors = new AtomicInteger();

    private AtomicInteger writeErrors = new AtomicInteger();

    @Override
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

            progressLogger = new MigrationProgressLogger();
            rateMonitor = new RateMonitor(readPermitsRef, writePermitsRef);
            if (System.getProperty("rate.monitor.2") != null) {
                rateMonitor = new RateMonitor2(readPermitsRef, writePermitsRef);
            }

            keyScanner = new KeyScanner(session);
            Set<Integer> scheduleIdsWith1HourData = keyScanner.scanFor1HourKeys();
            Set<Integer> scheduleIdsWith6HourData = keyScanner.scanFor6HourKeys();
            Set<Integer> scheduleIdsWith24HourData = keyScanner.scanFor24HourKeys();
            keyScanner.shutdown();

            log.info("There are " + scheduleIdsWith1HourData.size() + " schedule ids with " +
                Bucket.ONE_HOUR + " data");
            log.info("There are " + scheduleIdsWith6HourData.size() + " schedule ids with " +
                Bucket.SIX_HOUR + " data");
            log.info("There are " + scheduleIdsWith24HourData.size() + " schedule ids with " +
                Bucket.TWENTY_FOUR_HOUR + " data");

            writePermitsRef.set(RateLimiter.create(getWriteLimit(getNumberOfUpNodes()), DEFAULT_WARM_UP,
                TimeUnit.SECONDS));
            readPermitsRef.set(RateLimiter.create(getReadLimit(getNumberOfUpNodes()), DEFAULT_WARM_UP,
                TimeUnit.SECONDS));

            log.info("The request limits are " + writePermitsRef.get().getRate() + " writes/sec and " +
                readPermitsRef.get().getRate() + " reads/sec");

            remaining1HourMetrics.set(scheduleIdsWith1HourData.size());
            remaining6HourMetrics.set(scheduleIdsWith6HourData.size());
            remaining24HourMetrics.set(scheduleIdsWith24HourData.size());

            migrations = new CountDownLatch(scheduleIdsWith1HourData.size() + scheduleIdsWith6HourData.size() +
                scheduleIdsWith24HourData.size());

            threadPool.submit(progressLogger);
            threadPool.submit(rateMonitor);

            migrate1HourData(scheduleIdsWith1HourData);
            migrate6HourData(scheduleIdsWith6HourData);
            migrate24HourData(scheduleIdsWith24HourData);

            migrations.await();

            if (remaining1HourMetrics.get() > 0 || remaining6HourMetrics.get() > 0 ||
                remaining24HourMetrics.get() > 0) {
                throw new RuntimeException("There are unfinished metrics migrations - {one_hour: " +
                    remaining1HourMetrics + ", six_hour: " + remaining6HourMetrics + ", twenty_four_hour: " +
                    remaining24HourMetrics + "}. The upgrade will have to be " + "run again.");
            }

            dropTables();
        } catch (IOException e) {
            throw new RuntimeException("There was an unexpected I/O error. The are still " +
                migrations.getCount() + " outstanding migration tasks. The upgrade must be run again to " +
                "complete the migration.", e);
        } catch (AbortedException e) {
            throw new RuntimeException("The migration was aborted. There are are still " +
                migrations.getCount() +" outstanding migration tasks. The upgrade must be run again to " +
                "complete the migration.", e);
        } catch (InterruptedException e) {
            throw new RuntimeException("The migration was interrupted. There are are still " +
                migrations.getCount() +" outstanding migration tasks. The upgrade must be run again to " +
                "complete the migration.", e);
        } finally {
            stopwatch.stop();
            log.info("Finished migrating " + migrated1HourMetrics + " " + Bucket.ONE_HOUR + ", " +
                migrated6HourMetrics + " " + Bucket.SIX_HOUR + ", and " + migrated24HourMetrics + " " +
                Bucket.TWENTY_FOUR_HOUR + " metrics in " + stopwatch.elapsed(TimeUnit.SECONDS) + " sec");
            shutdown();
        }
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

    private void migrate1HourData(Set<Integer> scheduleIds) throws IOException {
        DateTime endTime = DateUtils.get1HourTimeSlice(DateTime.now());
        DateTime startTime = endTime.minus(Days.days(14));
        PreparedStatement query = session.prepare(
            "SELECT time, type, value FROM rhq.one_hour_metrics " +
            "WHERE schedule_id = ? AND time >= " + startTime.getMillis() + " AND time <= " + endTime.getMillis());
        PreparedStatement delete = session.prepare("DELETE FROM rhq.one_hour_metrics WHERE schedule_id = ?");
        migrateData(scheduleIds, query, delete, Bucket.ONE_HOUR, remaining1HourMetrics, migrated1HourMetrics,
            Days.days(14));
    }

    private void migrate6HourData(Set<Integer> scheduleIds) throws IOException {
        DateTime endTime = DateUtils.get1HourTimeSlice(DateTime.now());
        DateTime startTime = endTime.minus(Days.days(31));
        PreparedStatement query = session.prepare(
            "SELECT time, type, value FROM rhq.six_hour_metrics " +
            "WHERE schedule_id = ? AND time >= " + startTime.getMillis() + " AND time <= " + endTime.getMillis());
        PreparedStatement delete = session.prepare("DELETE FROM rhq.six_hour_metrics WHERE schedule_id = ?");
        migrateData(scheduleIds, query, delete, Bucket.SIX_HOUR, remaining6HourMetrics, migrated6HourMetrics,
            Days.days(31));
    }

    private void migrate24HourData(Set<Integer> scheduleIds) throws IOException {
        DateTime endTime = DateUtils.get1HourTimeSlice(DateTime.now());
        DateTime startTime = endTime.minus(Days.days(365));
        PreparedStatement query = session.prepare(
            "SELECT time, type, value FROM rhq.twenty_four_hour_metrics " +
            "WHERE schedule_id = ? AND time >= " + startTime.getMillis() + " AND time <= " + endTime.getMillis());
        PreparedStatement delete = session.prepare("DELETE FROM rhq.twenty_four_hour_metrics WHERE schedule_id = ?");
        migrateData(scheduleIds, query, delete, Bucket.TWENTY_FOUR_HOUR, remaining24HourMetrics, migrated24HourMetrics,
            Days.days(365));
    }

    private void migrateData(Set<Integer> scheduleIds, PreparedStatement query, final PreparedStatement delete, 
        Bucket bucket, final AtomicInteger remainingMetrics, final AtomicInteger migratedMetrics, Days ttl)
        throws IOException {

        Stopwatch stopwatch = Stopwatch.createStarted();
        Set<Integer> scheduleSet = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        Set<Integer> failedSet = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        scheduleSet.addAll(scheduleIds);
        log.info("Scheduling data migration tasks for " + bucket + " data");
        final MigrationLog migrationLog = new MigrationLog(new File(dataDir, bucket + "_migration.log"));
        final Set<Integer> migratedScheduleIds = migrationLog.read();

        int retryIterations = 0;
        while (!scheduleSet.isEmpty()) {
            log.info("Migrating .. retry iteration " + retryIterations);
            retryIterations++;
            Set<Future<?>> futures = new HashSet<Future<?>>();
            log.info("Scheduling " + scheduleSet.size() + " migrations ..");
            for (Integer scheduleId : scheduleIds) {
                if (migratedScheduleIds.contains(scheduleId)) {
                    migrations.countDown();
                    remainingMetrics.decrementAndGet();
                } else {
                    MigrateSchedule ms = new MigrateSchedule(session, readErrors, writeErrors, scheduleId, bucket,
                        query, scheduleSet, rateMonitor, migrationLog, readPermitsRef, writePermitsRef,
                        migrationsMeter, ttl, failedSet, migrations, remainingMetrics);
                    futures.add(threadPool.submit(ms));

                }
            }
            log.info("All migrations scheduled, awaiting results");
            for (Future<?> f : futures) {
                try {
                    f.get(3, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    log.info("TIMEOUT in getting migration result", e);
                }
            }
        }

        stopwatch.stop();
        log.info("Finished scheduling migration tasks for " + bucket + " data in " +
            stopwatch.elapsed(TimeUnit.SECONDS) + " sec");
    }

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

    private void dropTables() {
        ResultSet resultSet = session.execute("SELECT columnfamily_name FROM system.schema_columnfamilies " +
            "WHERE keyspace_name = 'rhq'");
        for (com.datastax.driver.core.Row row : resultSet) {
            String table = row.getString(0);
            if (table.equals("one_hour_metrics") || table.equals("six_hour_metrics") ||
                table.equals("twenty_four_hour_metrics")) {
                log.info("Dropping table " +  table);
                session.execute("DROP table rhq." + table);
            }
        }
    }

}
