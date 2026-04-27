package com.kafka.stream.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertEvent {
    private String userId;
    private long windowStart;
    private long windowEnd;
    private long eventCount;
}
