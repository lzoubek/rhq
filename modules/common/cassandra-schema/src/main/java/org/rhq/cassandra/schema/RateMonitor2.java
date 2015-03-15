package org.rhq.cassandra.schema;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * just another attempt to implement RateMonitor
 * @author Libor Zoubek
 */
public class RateMonitor2 extends RateMonitor {


    private static final Log log = LogFactory.getLog(RateMonitor2.class);

    private static class RequestStats {
        public double requests;
        public double failedRequests;
        public double failedRate = 0;

        public RequestStats(double requests, double failedRequests) {
            this.requests = requests;
            this.failedRequests = failedRequests;
            if (requests > 0) {
                this.failedRate = failedRequests / requests;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RequestStats that = (RequestStats) o;

            if (Double.compare(that.failedRequests, failedRequests) != 0) return false;
            if (Double.compare(that.requests, requests) != 0) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            temp = Double.doubleToLongBits(requests);
            result = (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(failedRequests);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }
    }


    private static final double MIN_WRITE_RATE = 100;
    private static final double RATE_RW_RATIO = 0.04;
    private static final double MIN_READ_RATE = MIN_WRITE_RATE * RATE_RW_RATIO;

    private static final int DEFAULT_WARM_UP = 10;

    private LinkedList<RequestStats> oneSecondStats = new LinkedList<RequestStats>();

    private int needBalanceTick = 0;

    private AtomicInteger requests = new AtomicInteger();

    private AtomicInteger failRequests = new AtomicInteger();

    private boolean shutdown;


    private AtomicReference<RateLimiter> readPermitsRef;

    private AtomicReference<RateLimiter> writePermitsRef;

    private double FAIL_TRESHOLD_5S_QUICK_DECREASE = 0.05;

    private double FAIL_TRESHOLD_5S_SLOW_DECREASE = 0.01;

    /**
     * our wake up time is 1s, this values sets how long are we going to ignore statistics. Basic idea comes from the fact, that we did decrease rate
     * and we did this because we got errors. Now when we decrease the rate, there might still be pending queries which are going to timeout (based on driver 
     * timeout setting), so we'd better ignore these errors, otherwise we'd decrease again (which may not be necessary)
     */
    private static final int DISABLED_AFTER_DICREASE = 15;

    private long sinceDecreasedRateTick = DISABLED_AFTER_DICREASE;

    public RateMonitor2(AtomicReference<RateLimiter> readPermitsRef, AtomicReference<RateLimiter> writePermitsRef) {
        super(readPermitsRef, writePermitsRef);
        this.readPermitsRef = readPermitsRef;
        this.writePermitsRef = writePermitsRef;
    }

    public void requestSucceeded() {
        requests.incrementAndGet();
    }

    public void requestFailed() {
        failRequests.incrementAndGet();
        requests.incrementAndGet();
    }

    public void shutdown() {
        shutdown = true;
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                if (requests.get() == 0) {
                    continue;
                }
                sinceDecreasedRateTick++;
                needBalanceTick++;
                oneSecondStats.addFirst(new RequestStats(requests.getAndSet(0), failRequests.getAndSet(0)));
                if (oneSecondStats.size() > 60) {
                    oneSecondStats.removeLast();
                }
                if (sinceDecreasedRateTick > DISABLED_AFTER_DICREASE && needBalanceTick > 9) {
                    needBalanceTick = 0;
                    balanceRates();
                }

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.info("Stopping request monitoring due to interrupt", e);
            } catch (Exception e) {
                log.warn("There was an unexpected error", e);
            }
        }
    }

    private void balanceRates() {
        if (oneSecondStats.size() < 30) {
            return;
        }
        double failureRate10s = getFailureRate(10);
        double failureRate30s = getFailureRate(30);
        double failureRate1m = getFailureRate(60);
        log.info("Balancing rates FR10s=" + failureRate10s + " FR30s=" + failureRate30s + " statsCount="
            + oneSecondStats.size());
        List<String> failureRates = new ArrayList<String>();
        for (RequestStats rs : oneSecondStats) {
            failureRates.add(new BigDecimal(rs.failedRate, new MathContext(2)).toString());
        }
        log.info(Arrays.toString(failureRates.toArray()));

        if (failureRate10s > FAIL_TRESHOLD_5S_QUICK_DECREASE) {
            // we need to quickly slow down as there are *many* errors
            decreaseRates(2);
            sinceDecreasedRateTick = 0;
            return;
        }
        if (failureRate10s > FAIL_TRESHOLD_5S_SLOW_DECREASE) {
            decreaseRates(1.1);
            sinceDecreasedRateTick = 0;
            return;
        }
        // failure between FAIL_TRESHOLD_5S_SLOW_DECREASE and 0 - we do nothing, this is optimal
        if (failureRate10s == 0) {
            // let's go faster, but how faster?

            if (failureRate30s == 0) {
                // we are very stable, let's increase very little
                if (failureRate1m == 0) {
                    increaseRates(1.05);
                    return;
                }
                increaseRates(1.1);
                return;

            }
            if (failureRate30s < 0.05) {
                increaseRates(1.2);
                return;
            }
            if (failureRate30s < 0.1) {
                increaseRates(1.3);
                return;
            }
            increaseRates(1.5);
        }
    }

    private double getFailureRate(int pastSeconds) {
        double rate = 0;
        int i = 0;
        if (oneSecondStats.isEmpty()) {
            return 0;
        }
        for (RequestStats rs : oneSecondStats) {
            if (i > pastSeconds) {
                break;
            }
            rate += rs.failedRate;
            i++;
        }
        return rate / i;
    }

    private void decreaseRates(double factor) {
        double readRate = readPermitsRef.get().getRate();
        double writeRate = Math.max(writePermitsRef.get().getRate(), MIN_WRITE_RATE);

        double newWriteRate = writeRate / factor;
        double newReadRate = Math.max(newWriteRate * RATE_RW_RATIO, MIN_READ_RATE);

        log.info("Decreasing request rates with " + factor + ":\n" +
            readRate + " reads/sec --> " + newReadRate + " reads/sec\n" +
            writeRate + " writes/sec --> " + newWriteRate + " writes/sec\n");

        readPermitsRef.set(RateLimiter.create(newReadRate, DEFAULT_WARM_UP, TimeUnit.SECONDS));
        writePermitsRef.set(RateLimiter.create(newWriteRate, DEFAULT_WARM_UP, TimeUnit.SECONDS));
    }


    private void increaseRates(double factor) {
        double readRate = readPermitsRef.get().getRate();
        double writeRate = writePermitsRef.get().getRate();

        double newWriteRate = writeRate * factor;
        double newReadRate = newWriteRate * RATE_RW_RATIO;

        log.info("Increasing request rates with " + factor + ":\n" +
            readRate + " reads/sec --> " + newReadRate + " reads/sec\n" +
            writeRate + " writes/sec --> " + newWriteRate + " writes/sec\n");

        readPermitsRef.set(RateLimiter.create(newReadRate, DEFAULT_WARM_UP, TimeUnit.SECONDS));
        writePermitsRef.set(RateLimiter.create(newWriteRate, DEFAULT_WARM_UP, TimeUnit.SECONDS));
    }

}
