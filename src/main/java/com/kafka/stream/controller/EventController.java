package com.kafka.stream.controller;

import com.kafka.stream.model.UserEvent;
import com.kafka.stream.service.EventService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class EventController {

    private final EventService eventService;

    @PostMapping
    public ResponseEntity<String> sendEvent(@RequestBody UserEvent event) {
        eventService.sendEvent(event);
        return ResponseEntity.accepted().body("Event sent for userId: " + event.getUserId());
    }

    @PostMapping("/simulate-fraud/{userId}")
    public ResponseEntity<String> simulateFraud(@PathVariable String userId) {
        long count = eventService.simulateFraud(userId);
        return ResponseEntity.accepted()
                .body("Sent " + count + " events for userId: " + userId + " — watch the alerts topic");
    }
}
