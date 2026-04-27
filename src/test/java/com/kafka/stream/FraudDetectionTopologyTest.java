package com.kafka.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.stream.model.UserEvent;
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

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FraudDetectionTopologyTest {

    private static final String INPUT_TOPIC = "user-events";
    private static final String OUTPUT_TOPIC = "alerts";
    private static final long THRESHOLD = 5L;
    private static final long WINDOW_MINUTES = 5L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, UserEvent> inputTopic;
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
        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, new StringSerializer(),
                (topic, data) -> {
                    try { return MAPPER.writeValueAsBytes(data); }
                    catch (Exception e) { throw new RuntimeException(e); }
                });
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldEmitAlertWhenThresholdReached() {
        Instant base = Instant.EPOCH;

        for (long i = 0; i < THRESHOLD; i++) {
            inputTopic.pipeInput("key", event("user-1", 100), base.plusSeconds(i * 30));
        }
        // advance stream time past the window so the window closes
        inputTopic.pipeInput("key", event("other", 1), base.plus(Duration.ofMinutes(WINDOW_MINUTES + 1)));

        assertFalse(outputTopic.isEmpty());
        String alert = outputTopic.readValue();
        assertTrue(alert.contains("user-1"));
        assertTrue(alert.contains("eventCount"));
    }

    @Test
    void shouldNotEmitAlertWhenBelowThreshold() {
        Instant base = Instant.EPOCH;

        for (long i = 0; i < THRESHOLD - 1; i++) {
            inputTopic.pipeInput("key", event("user-2", 100), base.plusSeconds(i * 30));
        }
        inputTopic.pipeInput("key", event("other", 1), base.plus(Duration.ofMinutes(WINDOW_MINUTES + 1)));

        assertTrue(outputTopic.isEmpty());
    }

    private static UserEvent event(String userId, int amount) {
        return new UserEvent(userId, amount, Instant.now().toEpochMilli());
    }
}
