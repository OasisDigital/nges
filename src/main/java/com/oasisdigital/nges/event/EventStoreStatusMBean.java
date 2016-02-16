package com.oasisdigital.nges.event;

import java.util.List;
import java.util.Map;

public interface EventStoreStatusMBean {

    List<Map<String, Object>> getLatestEvents();

    Long getLastEventId();

}
