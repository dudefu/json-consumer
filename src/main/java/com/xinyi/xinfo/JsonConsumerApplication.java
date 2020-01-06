package com.xinyi.xinfo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.xinyi.xinfo.runner.StartupRunner;

@SpringBootApplication
public class JsonConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(JsonConsumerApplication.class, args);
    }

    @Bean
    public StartupRunner startupRunner(){
        return new StartupRunner();
    }

}
