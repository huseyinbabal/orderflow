package com.orderflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling   // Session 4: the PEL sweeper runs on a fixed delay
public class OrderflowApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrderflowApplication.class, args);
	}

}
