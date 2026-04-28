package com.kafka.stream;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.List;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    private final StreamsBuilder streamsBuilder;
    private final List<KafkaTopology> topologies;

    public KafkaStreamsConfig(StreamsBuilder streamsBuilder, List<KafkaTopology> topologies) {
        this.streamsBuilder = streamsBuilder;
        this.topologies = topologies;
    }

    @PostConstruct
    public void registerTopologies() {
        topologies.forEach(t -> t.build(streamsBuilder));
    }
}
