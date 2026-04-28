package com.kafka.stream;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AlertConsumer {

    @KafkaListener(topics = "${app.fraud.output-topic}", groupId = "alert-logger")
    public void onAlert(String alertJson) {
        log.info(">>>>>>>>>> ALERT received on alerts topic: {} <<<<<<<<<<", alertJson);
    }
}
