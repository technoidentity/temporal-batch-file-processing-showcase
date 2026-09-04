package com.example.temporalshowcase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BatchFileProcessingApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchFileProcessingApplication.class, args);
    }
}
