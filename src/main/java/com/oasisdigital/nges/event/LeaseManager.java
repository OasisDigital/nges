package com.oasisdigital.nges.event;

import java.util.Collection;
import java.util.List;

public interface LeaseManager {

    /**
     * <p>
     * Lease a <code>leaseKey</code>, preventing concurrent computations and modifications on a data set for
     * specified time. The lease can be extended by calling this method again with the same keys. It should be
     * released with {@link #release(String, String)} as soon as it's not needed anymore, allowing other
     * processes to take action as soon as possible.
     * </p>
     *
     * <p>
     * The lease cannot expire while a {@link #save(List, String, long, Collection, String)} call is running.
     * In other words, it behaves atomically and guarantees that another thread cannot obtain it before the
     * events are committed.
     * </p>
     *
     * <p>
     * Lease is an alternative to locking which does not require holding a session open and minimizes impact
     * of runaway processes holding the lock indefinitely long. Like lock, a lease should be acquired before
     * reading data and making calculations in order to prevent two threads from conflicting computations.
     * </p>
     *
     * @see #release(String, String)
     * @see #save(List, String, long, Collection, String)
     *
     * @param leaseKey
     *            Identifier to lease. When it's granted to a process, no other process can claim it until
     *            it's released or expired.
     * @param ownerKey
     *            Identifier of a process (session) claiming the lease. It should be different for every
     *            session (thread, computation).
     * @param leaseDurationMs
     *            Time after which the key can be leased by another owner, unless renewed. Must be a positive
     *            integer shorter than 1 hour.
     * @return Information about the lease, including the current owner (which is <code>ownerKey</code> if it
     *         was successfully granted/renewed) and expiration date.
     */
    Lease lease(String leaseKey, String ownerKey, long leaseDurationMs);

    /**
     * Like {@link #lease(String, String, long)}, but the lease is only successful if the owner hasn't
     * changed. For example, if a lease had been acquired and released by owner A, then by B,
     * {@link #lease(String, String, long)} is still possible for A (creating a new lease) but renewal is not.
     *
     * @return Information about the lease, including the current owner (which is <code>ownerKey</code> if it
     *         was successfully granted/renewed) and expiration date.
     *
     * @throws EventStoreException
     *             If no lease with such a key exists (i.e. it's never been created).
     */
    Lease renewLease(String leaseKey, String ownerKey, long leaseDurationMs);

    /**
     * <p>
     * Release the lease with given key, if it's (still) currently owned by by given process
     * </p>
     *
     * <p>
     * See {@link #lease(String, String, long)}
     * </p>
     *
     * @param leaseKey
     * @param ownerKey
     */
    void release(String leaseKey, String ownerKey);
}
