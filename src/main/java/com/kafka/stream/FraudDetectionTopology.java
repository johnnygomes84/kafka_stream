package com.kafka.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.stream.model.AlertEvent;
import com.kafka.stream.model.UserEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
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
    public KStream<String, UserEvent> fraudDetectionStream(StreamsBuilder builder) {
        return buildTopology(builder, inputTopic, outputTopic, eventThreshold, windowMinutes);
    }

    static KStream<String, UserEvent> buildTopology(StreamsBuilder builder, String inputTopic,
            String outputTopic, long threshold, long windowMinutes) {

        Serde<UserEvent> userEventSerde = jsonSerde(UserEvent.class);

        KStream<String, UserEvent> userEventStream = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), userEventSerde))
                .selectKey((k, v) -> v.getUserId())
                .filter((k, v) -> k != null)
                .peek((k, v) -> log.info("Stream consumed event: userId={} amount={}", k, v.getAmount()));

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

        return userEventStream;
    }

    // TODO: Spring Kafka 4.0 deprecated JsonSerde/JsonSerializer/JsonDeserializer with no built-in replacement yet.
    // Using plain Jackson + Serdes.serdeFrom() until Spring Kafka provides a clean alternative.
    private static <T> Serde<T> jsonSerde(Class<T> type) {
        return Serdes.serdeFrom(
                (_, data) -> {
                    try {
                        return MAPPER.writeValueAsBytes(data);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to serialize " + type.getSimpleName(), e);
                    }
                },
                (_, data) -> {
                    try {
                        return data == null ? null : MAPPER.readValue(data, type);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to deserialize " + type.getSimpleName(), e);
                    }
                }
        );
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
