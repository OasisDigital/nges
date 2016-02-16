package com.oasisdigital.nges.event.util;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class LogThrottle {
    private final Cache<String, Object> cache;

    public LogThrottle(long duration, TimeUnit timeUnit) {
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(duration, timeUnit).build();
    }

    /**
     *
     * @return true if the exception should be throttled (has been written recently).
     */
    public boolean throttle(Exception exception) {
        String exceptionStr = exception.toString();
        boolean throttled = cache.getIfPresent(exceptionStr) != null;
        if (!throttled) {
            cache.put(exceptionStr, new Object());
        }
        return throttled;
    }
}
