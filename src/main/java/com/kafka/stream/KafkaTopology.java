package com.kafka.stream;

import org.apache.kafka.streams.StreamsBuilder;

public interface KafkaTopology {
    void build(StreamsBuilder builder);
}
