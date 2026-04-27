package com.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FraudDetectionTopologyTest {

    private static final String INPUT_TOPIC = "user-events";
    private static final String OUTPUT_TOPIC = "alerts";
    private static final long THRESHOLD = 5L;
    private static final long WINDOW_MINUTES = 5L;

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        FraudDetectionTopology.buildTopology(builder, INPUT_TOPIC, OUTPUT_TOPIC, THRESHOLD, WINDOW_MINUTES);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(builder.build(), props);
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldEmitAlertWhenThresholdReached() {
        Instant base = Instant.EPOCH;

        // send exactly THRESHOLD events for user-1 within one 5-minute window
        for (long i = 0; i < THRESHOLD; i++) {
            inputTopic.pipeInput("k", event("user-1", 100), base.plusSeconds(i * 30));
        }
        // advance stream time past the window so the count update is flushed
        inputTopic.pipeInput("k", event("other", 1), base.plusMinutes(WINDOW_MINUTES + 1));

        assertFalse(outputTopic.isEmpty());
        String alert = outputTopic.readValue();
        assertTrue(alert.contains("user-1"));
        assertTrue(alert.contains("eventCount"));
    }

    @Test
    void shouldNotEmitAlertWhenBelowThreshold() {
        Instant base = Instant.EPOCH;

        // send fewer events than the threshold
        for (long i = 0; i < THRESHOLD - 1; i++) {
            inputTopic.pipeInput("k", event("user-2", 100), base.plusSeconds(i * 30));
        }
        inputTopic.pipeInput("k", event("other", 1), base.plusMinutes(WINDOW_MINUTES + 1));

        assertTrue(outputTopic.isEmpty());
    }

    @Test
    void shouldIgnoreInvalidJsonEvents() {
        inputTopic.pipeInput("k", "not-valid-json", Instant.EPOCH);
        inputTopic.pipeInput("k", event("other", 1), Instant.EPOCH.plusMinutes(WINDOW_MINUTES + 1));

        assertTrue(outputTopic.isEmpty());
    }

    private static String event(String userId, int amount) {
        return String.format("{\"userId\":\"%s\",\"amount\":%d,\"timestamp\":%d}",
                userId, amount, Instant.now().toEpochMilli());
    }
}
