package com.kafka.stream.service;

import com.kafka.stream.model.UserEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventService {

    private final KafkaTemplate<String, UserEvent> kafkaTemplate;

    @Value("${app.fraud.input-topic:user-events}")
    private String inputTopic;

    @Value("${app.fraud.event-threshold:5}")
    private long eventThreshold;

    public void sendEvent(UserEvent event) {
        if (event.getTimestamp() == 0) {
            event.setTimestamp(System.currentTimeMillis());
        }
        kafkaTemplate.send(inputTopic, event.getUserId(), event);
        log.info("Single event sent: userId={} amount={}", event.getUserId(), event.getAmount());
    }

    public long simulateFraud(String userId) {
        long count = eventThreshold + 1;
        for (int i = 0; i < count; i++) {
            kafkaTemplate.send(inputTopic, userId, new UserEvent(userId, 100.0, System.currentTimeMillis()));
        }
        log.info("Fraud simulation: sent {} events for userId={}", count, userId);
        return count;
    }
}
