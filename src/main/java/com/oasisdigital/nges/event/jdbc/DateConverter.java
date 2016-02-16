package com.oasisdigital.nges.event.jdbc;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Optional;

class DateConverter {
    public static OffsetDateTime toOffsetDateTime(Timestamp timestamp) {
        return Optional.ofNullable(timestamp)
                .map(ts -> OffsetDateTime.ofInstant(ts.toInstant(), ZoneId.systemDefault())).orElse(null);
    }
}
