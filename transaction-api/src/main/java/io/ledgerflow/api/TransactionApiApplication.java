package io.ledgerflow.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class TransactionApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(TransactionApiApplication.class, args);
    }
}

