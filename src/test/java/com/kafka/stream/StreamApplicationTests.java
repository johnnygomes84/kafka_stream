package com.kafka.stream;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
		"spring.kafka.streams.auto-startup=false",
		"spring.kafka.admin.fail-fast=false"
})
class StreamApplicationTests {

	@Test
	void contextLoads() {
	}

}
