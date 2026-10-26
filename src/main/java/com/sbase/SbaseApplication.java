package com.sbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class SbaseApplication {
    public static void main(String[] args) {
        SpringApplication.run(SbaseApplication.class, args);
    }
}
