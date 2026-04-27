package com.kafka.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.stream.model.AlertEvent;
import com.kafka.stream.model.UserEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Configuration
@EnableKafkaStreams
public class FraudDetectionTopology {

    @Value("${app.fraud.input-topic:user-events}")
    private String inputTopic;

    @Value("${app.fraud.output-topic:alerts}")
    private String outputTopic;

    @Value("${app.fraud.event-threshold:5}")
    private long eventThreshold;

    @Value("${app.fraud.window-minutes:5}")
    private long windowMinutes;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Bean
    public KStream<String, String> fraudDetectionStream(StreamsBuilder builder) {
        return buildTopology(builder, inputTopic, outputTopic, eventThreshold, windowMinutes);
    }

    static KStream<String, String> buildTopology(StreamsBuilder builder, String inputTopic,
            String outputTopic, long threshold, long windowMinutes) {

        JsonSerde<UserEvent> userEventSerde = new JsonSerde<>(UserEvent.class);

        KStream<String, String> rawStream = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, UserEvent> userEventStream = rawStream
                .mapValues(FraudDetectionTopology::parseEvent)
                .filter((k, v) -> v != null)
                .selectKey((k, v) -> v.getUserId())
                .filter((k, v) -> k != null);

        TimeWindows window = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes));

        userEventStream
                .groupByKey(Grouped.with(Serdes.String(), userEventSerde))
                .windowedBy(window)
                .count(Materialized.as("event-count-store"))
                .toStream()
                .filter((windowed, count) -> count != null && count >= threshold)
                .map((windowed, count) -> KeyValue.pair(windowed.key(), toAlertJson(windowed, count)))
                .filter((k, v) -> v != null)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return rawStream;
    }

    private static UserEvent parseEvent(String json) {
        try {
            return MAPPER.readValue(json, UserEvent.class);
        } catch (Exception e) {
            log.warn("Skipping unparseable event: {}", json);
            return null;
        }
    }

    private static String toAlertJson(Windowed<String> windowed, Long count) {
        try {
            AlertEvent alert = AlertEvent.builder()
                    .userId(windowed.key())
                    .windowStart(windowed.window().start())
                    .windowEnd(windowed.window().end())
                    .eventCount(count)
                    .build();
            log.info("ALERT: userId={} window=[{} -> {}] count={}",
                    alert.getUserId(),
                    Instant.ofEpochMilli(alert.getWindowStart()),
                    Instant.ofEpochMilli(alert.getWindowEnd()),
                    alert.getEventCount());
            return MAPPER.writeValueAsString(alert);
        } catch (Exception e) {
            log.error("Failed to build alert for userId={}", windowed.key(), e);
            return null;
        }
    }
}
