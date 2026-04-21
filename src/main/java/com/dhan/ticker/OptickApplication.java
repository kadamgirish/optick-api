package com.dhan.ticker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OptickApplication {

    public static void main(String[] args) {
        SpringApplication.run(OptickApplication.class, args);
    }
}
