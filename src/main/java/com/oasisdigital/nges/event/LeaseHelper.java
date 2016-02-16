package com.oasisdigital.nges.event;

import static java.lang.Math.min;
import static java.time.OffsetDateTime.now;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.concurrent.TimeUnit;

public class LeaseHelper {
    private static final long ATTEMPT_INTERVAL_MS = 150;

    /**
     * Attempts to obtain lease by querying EventStore#lease(String, String, long) repeatedly, trying for up
     * to maxWaitMs milliseconds.
     *
     * @throws InterruptedException
     */
    public static Lease lease(LeaseManager manager, String leaseKey, String ownerKey, long leaseDurationMs,
            long maxWaitMs) throws InterruptedException {
        OffsetDateTime endTime = now().plusNanos(TimeUnit.MILLISECONDS.toNanos(maxWaitMs));
        Lease lease;
        do {
            lease = manager.lease(leaseKey, ownerKey, leaseDurationMs);
            if (lease.isOwnedBy(ownerKey)) {
                return lease;
            } else {
                OffsetDateTime now = now();
                long tillExpiration = Duration.between(now, lease.getExpirationDate()).abs().toMillis();
                long tillMaxWait = Duration.between(now, endTime).abs().toMillis();
                long sleepTime = min(min(tillExpiration, tillMaxWait), ATTEMPT_INTERVAL_MS);
                Thread.sleep(sleepTime);
            }
        } while (now().isBefore(endTime));
        return lease;
    }
}
